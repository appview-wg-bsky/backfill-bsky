import { iterateAtpRepo } from "@atcute/car";
import {
	type HeadersObject,
	simpleFetchHandler,
	XRPC,
	XRPCError,
	type XRPCRequestOptions,
	type XRPCResponse,
} from "@atcute/client";
import { parse as parseTID } from "@atcute/tid";
import * as bsky from "@atproto/bsky";
import { IdResolver, MemoryCache } from "@atproto/identity";
import { AtUri } from "@atproto/syntax";
import { createClient } from "@redis/client";
import Queue from "bee-queue";
import CacheableLookup from "cacheable-lookup";
import { CID } from "multiformats/cid";
import cluster from "node:cluster";
import fs from "node:fs";
import * as os from "node:os";
import * as shm from "shm-typed-array";
import { Agent, setGlobalDispatcher } from "undici";
import { fetchAllDids, sleep } from "../util/fetch.js";
import type { CommitData } from "./main.js";

const cacheable = new CacheableLookup();

setGlobalDispatcher(
	new Agent({
		keepAliveTimeout: 300_000,
		connect: {
			timeout: 300_000,
			lookup: (hostname, { family: _family, hints, all, ..._options }, callback) => {
				const family = !_family ? undefined : (_family === 6 || _family === "IPv6") ? 6 : 4;
				return cacheable.lookup(hostname, {
					..._options,
					...(family ? { family } : {}),
					...(hints ? { hints } : {}),
					...(all ? { all } : {}),
				}, callback);
			},
		},
	}),
);

const repoProcessingQueue = new Queue<{ did: string }>("repo-processing", {
	removeOnSuccess: true,
	removeOnFailure: true,
});
const writeQueue = new Queue<{ out: CommitData[]; did: string }>("write-commits", {
	removeOnSuccess: true,
	removeOnFailure: true,
});

if (cluster.isPrimary) {
	const db = new bsky.Database({
		url: process.env.BSKY_DB_POSTGRES_URL,
		schema: process.env.BSKY_DB_POSTGRES_SCHEMA,
		poolSize: 50,
	});

	const idResolver = new IdResolver({
		plcUrl: process.env.BSKY_DID_PLC_URL,
		didCache: new MemoryCache(),
	});

	const { indexingSvc } = new bsky.RepoSubscription({
		service: process.env.BSKY_REPO_PROVIDER,
		db,
		idResolver,
	});

	const redis = createClient();
	await redis.connect();

	const numCPUs = os.availableParallelism();

	for (let i = 0; i < numCPUs; i++) {
		cluster.fork();
	}

	cluster.on("exit", (worker, code, signal) => {
		console.error(`${worker.process.pid} died with code ${code} and signal ${signal}`);
		cluster.fork();
	});

	writeQueue.process(
		100,
		(
			job: Queue.Job<{ out: CommitData[]; did: string }>,
			done: (err: any, data?: any) => void,
		) => {
			const { out, did } = job.data;
			console.time(`Writing records: ${out.length} for ${did}`);
			db.transaction(async (txn) => {
				const idx = indexingSvc.transact(txn);
				const insertHandle = idx.indexHandle(did, new Date().toISOString());
				const insertRecords = out.map(({ uri: _uri, cid, indexedAt, record }) => {
					const uri = new AtUri(_uri);
					const indexer = idx.findIndexerForCollection(uri.collection);
					if (indexer) {
						return indexer.insertRecord(uri, CID.parse(cid), record, indexedAt);
					}
				});
				await Promise.allSettled([insertHandle, ...insertRecords]);
			}).then(() => redis.sAdd("backfill:seen", did)).catch((err) =>
				console.error(`Error when writing ${did}`, err)
			).finally(() => {
				console.timeEnd(`Writing records: ${out.length} for ${did}`);
				done(null);
			});
		},
	);

	async function main() {
		console.log("Reading DIDs");
		const repos = await readOrFetchDids();
		console.log(`Filtering out seen DIDs from ${repos.length} total`);
		const notSeen = await redis.smIsMember("backfill:seen", repos.map((repo) => repo[0])).then((
			seen,
		) => repos.filter((_, i) => !seen[i]));
		console.log(`Queuing ${notSeen.length} repos for processing`);
		for (const dids of batch(notSeen, 100)) {
			await Promise.allSettled(dids.map(async ([did, pds]) => {
				const repo = await getRepo(pds, did as `did:${string}`);
				if (repo) {
					const shared = shm.create(repo.length, "Uint8Array", did);
					if (shared) shared.set(repo);
					await repoProcessingQueue.createJob({ did }).setId(did).save();
					console.timeEnd(`Fetching repo: ${did}`);
				}
			}));
		}

		await new Promise<void>((resolve) => {
			const checkQueue = async () => {
				const processJobCounts = await repoProcessingQueue.checkHealth();
				if (processJobCounts.waiting === 0 && processJobCounts.active === 0) {
					resolve();
				} else {
					setTimeout(checkQueue, 1000);
				}
			};
			checkQueue();
		});
	}

	void main();
} else {
	repoProcessingQueue.process(async (job) => {
		const { did } = job.data;

		if (!did || typeof did !== "string") {
			console.warn(`Invalid job data for ${job.id}: ${JSON.stringify(job.data)}`);
			return;
		}

		const repo = shm.get(did, "Uint8Array");
		if (!repo?.byteLength) {
			console.warn(`Did not get repo for ${did}`);
			return;
		}

		console.time(`Processing repo: ${did}`);

		try {
			const out = [];
			const now = Date.now();
			for await (const { record, rkey, collection, cid } of iterateAtpRepo(repo)) {
				const uri = `at://${did}/${collection}/${rkey}`;

				// This should be the date the AppView saw the record, but since we don't want the "archived post" label
				// to show for every post in social-app, we'll try our best to figure out when the record was actually created.
				// So first we try createdAt then parse the rkey; if either of those is in the future, we'll use now.
				let indexedAt: number =
					(!!record && typeof record === "object" && "createdAt" in record
						&& typeof record.createdAt === "string"
						&& new Date(record.createdAt).getTime()) || 0;
				if (!indexedAt || isNaN(indexedAt)) {
					try {
						indexedAt = parseTID(rkey).timestamp;
					} catch {
						indexedAt = now;
					}
				}
				if (indexedAt > now) indexedAt = now;

				const commit = {
					uri,
					cid: cid.$link,
					indexedAt: new Date(indexedAt).toISOString(),
					record,
				} satisfies CommitData;
				out.push(commit);
			}

			const writeJob = writeQueue.createJob({ out, did });
			await writeJob.save();
		} catch (err) {
			console.warn(`iterateAtpRepo error for did ${did} --- ${err}`);
		} finally {
			console.timeEnd(`Processing repo: ${did}`);
			shm.destroy(did);
		}
	});

	repoProcessingQueue.on("error", (err) => {
		console.error("Queue error:", err);
	});

	repoProcessingQueue.on("failed", (job, err) => {
		console.error(`Job failed for ${job.data.did}:`, err);
	});
}

class WrappedRPC extends XRPC {
	constructor(public service: string) {
		super({ handler: simpleFetchHandler({ service }) });
	}

	override async request(options: XRPCRequestOptions, attempt = 0): Promise<XRPCResponse> {
		const url = new URL("/xrpc/" + options.nsid, this.service).href;

		const request = async () => {
			const res = await super.request(options);
			await processRatelimitHeaders(res.headers, url, sleep);
			return res;
		};

		try {
			return await request();
		} catch (err) {
			if (attempt > 6) throw err;

			if (err instanceof XRPCError) {
				if (err.status === 429) {
					await processRatelimitHeaders(err.headers, url, sleep);
				} else throw err;
			} else if (err instanceof TypeError) {
				console.warn(`fetch failed for ${url}, skipping`);
				throw err;
			} else {
				await sleep(backoffs[attempt] || 60000);
			}
			console.warn(`Retrying request to ${url}, on attempt ${attempt}`);
			return this.request(options, attempt + 1);
		}
	}
}

async function getRepo(pds: string, did: `did:${string}`) {
	const rpc = new WrappedRPC(pds);
	try {
		const { data } = await rpc.get("com.atproto.sync.getRepo", { params: { did } });
		return data;
	} catch (err) {
		console.error(`getRepo error for did ${did} from pds ${pds} --- ${err}`);
	}
}

async function readOrFetchDids(): Promise<Array<[string, string]>> {
	try {
		return JSON.parse(fs.readFileSync("dids.json", "utf-8"));
	} catch (err: any) {
		const dids = await fetchAllDids();
		writeDids(dids);
		return dids;
	}
}

function writeDids(dids: Array<[string, string]>) {
	fs.writeFileSync("dids.json", JSON.stringify(dids));
}

function batch<T>(array: Array<T>, size: number): Array<Array<T>> {
	const result = [];
	for (let i = 0; i < array.length; i += size) {
		result.push(array.slice(i, i + size));
	}
	return result;
}

const backoffs = [1_000, 5_000, 15_000, 30_000, 60_000, 120_000, 300_000];

async function processRatelimitHeaders(
	headers: HeadersObject,
	url: string,
	onRatelimit: (wait: number) => unknown,
) {
	const remainingHeader = headers["ratelimit-remaining"],
		resetHeader = headers["ratelimit-reset"];
	if (!remainingHeader || !resetHeader) return;

	const ratelimitRemaining = parseInt(remainingHeader);
	if (isNaN(ratelimitRemaining) || ratelimitRemaining <= 1) {
		const ratelimitReset = parseInt(resetHeader) * 1000;
		if (isNaN(ratelimitReset)) {
			console.error("ratelimit-reset header is not a number at url " + url);
		} else {
			const now = Date.now();
			const waitTime = ratelimitReset - now + 1000; // add 1s to be safe
			if (waitTime > 0) {
				await onRatelimit(waitTime);
			}
		}
	}
}
