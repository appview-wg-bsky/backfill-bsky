import { readFileSync, writeFileSync } from "node:fs";
import PQueue from "p-queue";
import { errors, type Headers } from "undici";

export async function fetchPdses(): Promise<Array<string>> {
	const data = await fetch(
		"https://raw.githubusercontent.com/mary-ext/atproto-scraping/refs/heads/trunk/state.json",
	).then((res) => res.ok ? res.json() as any : null);

	if (!data.pdses) throw new Error("Failed to fetch PDSes");

	const pdses = Object.keys(data.pdses).filter((pds) => pds.startsWith("https://"));
	return pdses;
}

export async function* fetchAllDids() {
	const pdses = await fetchPdses();

	const cursors = getPdsCursorCache();
	const pdsesToFetchFrom = pdses.filter((pds) => cursors[pds] !== "DONE");

	yield* roundRobinInterleaveIterators(pdsesToFetchFrom.map(fetchPdsDids));
}

const listReposQueue = new PQueue({ concurrency: 25 });

async function* fetchPdsDids(pds: string) {
	let cursor = getPdsCursorCache()?.[pds] ?? "";
	if (cursor === "DONE") return console.warn(`Skipping exhausted PDS ${pds}`);
	const url = new URL(`/xrpc/com.atproto.sync.listRepos`, pds).href;
	let fetched = 0;
	while (true) {
		try {
			const res = await listReposQueue.add(() =>
				fetch(url + "?limit=1000&cursor=" + cursor, { signal: AbortSignal.timeout(10_000) })
			);
			if (!res?.ok) {
				if (res?.status === 429) {
					await processRatelimitHeaders(res.headers, url);
					continue;
				}
				throw new Error(
					`Failed to fetch DIDs from ${pds}: ${res?.status ?? "unknown"} ${
						res?.statusText ?? ""
					}`,
				);
			}

			const { cursor: _c, repos } = await res.json() as {
				cursor: string;
				repos: Array<{ did: string }>;
			};
			for (const repo of repos) {
				if (!repo.did) continue;
				yield [repo.did, pds] as const;
				fetched++;
			}

			if (!_c || _c === cursor) break;
			pdsCursorCache[pds] = cursor = _c;
			savePdsCursorCache();
		} catch (err: any) {
			const undiciError = err instanceof errors.UndiciError
				? err
				: (err instanceof Error && err.cause instanceof errors.UndiciError)
				? err.cause
				: null;
			if (
				[
					"ETIMEDOUT",
					"UND_ERR_CONNECT_TIMEOUT",
					"UND_ERR_HEADERS_TIMEOUT",
					"UND_ERR_SOCKET",
				].includes(undiciError?.code ?? "")
			) {
				console.warn(`Could not connect to ${url} for listRepos, skipping`);
				break;
			} else {
				// bsky.network PDS definitely exists
				if (pds.includes("bsky.network")) {
					console.warn(`listRepos failed for ${url} at cursor ${cursor}, retrying`);
					await sleep(5000);
				} else {
					console.warn(
						`listRepos failed for ${url} at cursor ${cursor}, skipping`,
						err.message || err,
					);
					break;
				}
			}
		}
	}
	console.log(`Exhausted ${pds}: fetched ${fetched} DIDs, ended at cursor ${cursor}`);
	pdsCursorCache[pds] = "DONE";
	savePdsCursorCache();
	return fetched;
}

// async function fetchPlcDids(map: Map<string, string> = new Map()): Promise<Map<string, string>> {
// 	let cursor = "";
// 	while (true) {
// 		console.log(`fetching plc dids, now ${map.size}`);
// 		const res = await fetch(`https://plc.directory/export?limit=1000%after=${cursor}`);
// 		if (!res.ok) {
// 			if (res.status === 429) {
// 				await sleep(10_000);
// 				continue;
// 			}
// 			throw new Error(`Failed to fetch PLC DIDs: ${res.status} ${res.statusText}`);
// 		}
//
// 		const lines = await res.text();
// 		const operations = lines.split("\n").map((line) => {
// 			try {
// 				return JSON.parse(line);
// 			} catch (e) {
// 				return null;
// 			}
// 		});
//
// 		for (const op of operations) {
// 			if (!op?.operation?.type) continue;
// 			if (op.operation.type === "create" && op.operation.service) {
// 				map.set(op.did, op.operation.service);
// 			} else if (op.operation.type === "plc_operation") {
// 				const pds = op.operation.services.atproto_pds.endpoint;
// 				if (pds) map.set(op.did, pds);
// 			} else if (op.operation.type === "plc_tombstone") map.delete(op.did);
// 		}
//
// 		cursor = operations.at(-1)?.createdAt;
// 		if (!cursor) break;
// 	}
//
// 	return map;
// }
//
// async function fetchWebDids(map: Map<string, string> = new Map()): Promise<Map<string, string>> {
// 	const data = await fetch(
// 		"https://raw.githubusercontent.com/mary-ext/atproto-scraping/refs/heads/trunk/state.json",
// 	).then((res) => res.ok ? res.json() as any : null);
// 	if (!data?.firehose?.didWebs) throw new Error("Failed to fetch web DIDs");
// 	for (const [did, { pds }] of Object.entries<{ pds: string }>(data.firehose.didWebs)) {
// 		map.set(did, pds);
// 	}
// 	return map;
// }

let pdsCursorCache: Record<string, string>;
const getPdsCursorCache =
	() => (pdsCursorCache ??= JSON.parse(readFileSync("./pds-cursor-cache.json", "utf8")) as Record<
		string,
		string
	>);
const savePdsCursorCache = () =>
	writeFileSync("./pds-cursor-cache.json", JSON.stringify(pdsCursorCache));

async function processRatelimitHeaders(headers: Headers, url: string) {
	const remainingHeader = headers.get("ratelimit-remaining"),
		resetHeader = headers.get("ratelimit-reset");
	if (!remainingHeader || !resetHeader) return;

	const ratelimitRemaining = parseInt(remainingHeader);
	if (isNaN(ratelimitRemaining) || ratelimitRemaining <= 1) {
		const ratelimitReset = parseInt(resetHeader) * 1000;
		if (isNaN(ratelimitReset)) {
			console.error("ratelimit-reset header is not a number at url " + url);
		} else {
			const now = Date.now();
			const waitTime = ratelimitReset - now + 1000; // add a second to be safe
			if (waitTime > 0) {
				await sleep(waitTime);
			}
		}
	}
}

export const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

export async function* roundRobinInterleaveIterators<T>(
	iterators: Array<AsyncIterator<T>>,
	concurrency = 25,
) {
	const getNext = (it: AsyncIterator<T>, idx: number) =>
		it.next().then((res) => ({ idx, res, error: null })).catch((error) => ({
			idx,
			res: null,
			error,
		}));

	// Queue of iterator indices waiting for their next turn
	const pending: number[] = iterators.map((_, i) => i);

	// Active promises by iterator index
	const activeMap = new Map<number, Promise<any>>();

	const launch = () => {
		while (activeMap.size < concurrency && pending.length) {
			const idx = pending.shift()!;
			const promise = getNext(iterators[idx], idx);
			activeMap.set(idx, promise);
		}
	};

	launch();

	const doneIterators = new Set<number>();

	while (doneIterators.size < iterators.length) {
		if (activeMap.size === 0) break;

		const result = await Promise.race(activeMap.values());
		activeMap.delete(result.idx);

		if (result.error) {
			console.error(result.error);
		}

		if (result.res.done) {
			doneIterators.add(result.idx);
		} else {
			// Emit value and put this iterator back in queue
			yield result.res.value;
			pending.push(result.idx);
		}

		// Top up the active promises pool to ensure we're operating at concurrency limit
		launch();
	}
}
