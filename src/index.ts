/**
 * HubInsight Worker
 * 
 * This worker fetches Docker Hub pull counts for specified images hourly
 * and stores the data in Cloudflare Analytics Engine for visualization.
 */

interface Env {
	DOCKER_PULLS_DATASET: AnalyticsEngineDataset;
	DOCKER_CONFIG: {
		repositories: Array<DockerRepository>;
	};
	ACCOUNT_ID: string;
	API_TOKEN: string;
}

// Docker Hub repository interface
interface DockerRepository {
	org: string;
	repo: string;
}

// Docker Hub API response interface
interface DockerHubResponse {
	pull_count: number;
	[key: string]: unknown;
}

// Result interface for collected data
interface DockerHubResult {
	timestamp: string;
	org: string;
	repo: string;
	pullCount: number;
}

// Combined stats interface for displaying data for different time periods
interface DockerHubCombinedStats {
	org: string;
	repo: string;
	totalPulls: number;
	oneDayPulls: number;
	sevenDayPulls: number;
	thirtyDayPulls: number;
}

export default {
	async fetch(req, env) {
		// Keep the fetch handler for local testing
		const url = new URL(req.url);

		// For manual testing, if path is /test-fetch, run the data fetch
		if (url.pathname === '/test-fetch') {
			try {
				const results = await fetchDockerHubStats(env);
				for (const result of results) {
					env.DOCKER_PULLS_DATASET.writeDataPoint({
						blobs: [result.org, result.repo],
						doubles: [result.pullCount],
						indexes: [result.org + result.repo]
					});
				}
				return new Response(JSON.stringify(results, null, 2), {
					headers: { 'Content-Type': 'application/json' }
				});
			} catch (error: unknown) {
				const errorMessage = error instanceof Error ? error.message : 'Unknown error';
				return new Response(`Error: ${errorMessage}`, { status: 500 });
			}
		}

		// Main page with combined stats for 1 day, 7 days, and 30 days in a single table
		if (url.pathname === '/') {
			try {
				const stats = await getDockerHubCombinedStats(env);
				return new Response(generateStatsHtml(stats), {
					headers: { 'Content-Type': 'text/html' }
				});
			} catch (error: unknown) {
				const errorMessage = error instanceof Error ? error.message : 'Unknown error';
				return new Response(`Error: ${errorMessage}`, { status: 500 });
			}
		}
		return new Response('Not found', { status: 404 });
	},

	// Runs hourly to fetch Docker Hub stats and save to Analytics Engine
	async scheduled(event, env, ctx): Promise<void> {
		console.log(`Scheduled trigger fired at ${event.cron}`);

		try {
			const results = await fetchDockerHubStats(env);

			// Save each result to Analytics Engine
			for (const result of results) {
				env.DOCKER_PULLS_DATASET.writeDataPoint({
					blobs: [result.org, result.repo],
					doubles: [result.pullCount],
					indexes: [result.org + result.repo]
				});
			}

			console.log(`Successfully saved ${results.length} data points to Analytics Engine`);
		} catch (error: unknown) {
			const errorMessage = error instanceof Error ? error.message : 'Unknown error';
			console.error(`Error processing Docker Hub stats: ${errorMessage}`);
		}
	},
} satisfies ExportedHandler<Env>;

// Function to fetch Docker Hub statistics
async function fetchDockerHubStats(env: { DOCKER_CONFIG: { repositories: Array<DockerRepository> } }): Promise<DockerHubResult[]> {
	const results: DockerHubResult[] = [];

	// Get Docker repositories from environment config
	const dockerImages = env.DOCKER_CONFIG.repositories;

	if (!dockerImages || dockerImages.length === 0) {
		console.warn('No Docker repositories configured in DOCKER_CONFIG');
		return results;
	}

	for (const image of dockerImages) {
		try {
			const { org, repo } = image;
			const url = `https://hub.docker.com/v2/repositories/${org}/${repo}`;
			const response = await fetch(url);

			if (!response.ok) {
				throw new Error(`HTTP error! status: ${response.status}`);
			}

			const data = await response.json() as DockerHubResponse;

			results.push({
				timestamp: new Date().toISOString(),
				org,
				repo,
				pullCount: data.pull_count
			});

			// Add a small delay to avoid rate limiting
			await new Promise(resolve => setTimeout(resolve, 100));
		} catch (error: unknown) {
			const errorMessage = error instanceof Error ? error.message : 'Unknown error';
			console.error(`Error fetching data for ${image.org}/${image.repo}: ${errorMessage}`);
		}
	}

	return results;
}

// Function to query Analytics Engine for historical Docker Hub data
async function queryAnalyticsEngine(env: Env, query: string): Promise<any> {
	const API = `https://api.cloudflare.com/client/v4/accounts/${env.ACCOUNT_ID}/analytics_engine/sql`;
	const response = await fetch(API, {
		method: "POST",
		headers: {
			"Authorization": `Bearer ${env.API_TOKEN}`,
			"Content-Type": "application/json"
		},
		body: query
	});

	if (!response.ok) {
		throw new Error(`Analytics Engine query failed: status ${response.status}`);
	}

	return await response.json();
}

// Get the data point for a specific day interval for a single repository
async function getDockerPullsForSingleRepo(env: Env, org: string, repo: string, days: number): Promise<{ org: string; repo: string; pull_count: number; timestamp: string } | null> {
	try {
		const query = `
			SELECT 
				blob1 AS org,
				blob2 AS repo,
				double1 AS pull_count,
				timestamp
			FROM docker_pulls 
			WHERE org = '${org}'
			AND repo = '${repo}'
			AND timestamp > NOW() - INTERVAL '${days}' DAY
			ORDER BY timestamp
			LIMIT 1
		`;
		const result = await queryAnalyticsEngine(env, query);

		if (result.data && result.data.length > 0) {
			return result.data[0];
		}
		return null;
	} catch (error) {
		console.error(`Error querying pull data for ${org}/${repo} for last ${days} days: ${error instanceof Error ? error.message : 'Unknown error'}`);
		return null;
	}
}

// Get the data points for a specific day interval for all repositories
async function getDockerPullsForInterval(env: Env, days: number): Promise<Array<{ org: string; repo: string; pull_count: number; timestamp: string }>> {
	try {
		const repositories = env.DOCKER_CONFIG.repositories;

		// 并发查询所有仓库数据
		const promises = repositories.map(repository =>
			getDockerPullsForSingleRepo(env, repository.org, repository.repo, days)
		);

		// 等待所有查询完成
		const results = await Promise.all(promises);

		// 过滤掉 null 结果
		return results.filter(result => result !== null) as Array<{ org: string; repo: string; pull_count: number; timestamp: string }>;
	} catch (error) {
		console.error(`Error querying pull data for last ${days} days: ${error instanceof Error ? error.message : 'Unknown error'}`);
		return [];
	}
}

// Function to fetch combined Docker Hub stats for all time periods
async function getDockerHubCombinedStats(env: Env): Promise<DockerHubCombinedStats[]> {
	try {
		// 并发获取当前数据和三个时间段的历史数据
		const [
			currentResults,
			oneDayData,
			sevenDayData,
			thirtyDayData
		] = await Promise.all([
			fetchDockerHubStats(env),
			getDockerPullsForInterval(env, 1),
			getDockerPullsForInterval(env, 7),
			getDockerPullsForInterval(env, 30)
		]);

		// Create lookup maps for easier access
		const oneDayMap = new Map();
		const sevenDayMap = new Map();
		const thirtyDayMap = new Map();

		// Process data from each interval
		oneDayData.forEach((item: { org: string; repo: string; pull_count: number; timestamp: string }) => {
			const key = `${item.org}/${item.repo}`;
			oneDayMap.set(key, item);
		});

		sevenDayData.forEach((item: { org: string; repo: string; pull_count: number; timestamp: string }) => {
			const key = `${item.org}/${item.repo}`;
			sevenDayMap.set(key, item);
		});

		thirtyDayData.forEach((item: { org: string; repo: string; pull_count: number; timestamp: string }) => {
			const key = `${item.org}/${item.repo}`;
			thirtyDayMap.set(key, item);
		});

		// Combine the data
		return currentResults.map(current => {
			const key = `${current.org}/${current.repo}`;
			const oneDayItem = oneDayMap.get(key);
			const sevenDayItem = sevenDayMap.get(key);
			const thirtyDayItem = thirtyDayMap.get(key);

			// Calculate increases
			const oneDayPulls = oneDayItem ? (current.pullCount - oneDayItem.pull_count) : 0;
			const sevenDayPulls = sevenDayItem ? (current.pullCount - sevenDayItem.pull_count) : 0;
			const thirtyDayPulls = thirtyDayItem ? (current.pullCount - thirtyDayItem.pull_count) : 0;

			return {
				org: current.org,
				repo: current.repo,
				totalPulls: current.pullCount,
				oneDayPulls: oneDayPulls > 0 ? oneDayPulls : 0,
				sevenDayPulls: sevenDayPulls > 0 ? sevenDayPulls : 0,
				thirtyDayPulls: thirtyDayPulls > 0 ? thirtyDayPulls : 0
			};
		});
	} catch (error) {
		console.error(`Error processing combined Docker Hub stats: ${error instanceof Error ? error.message : 'Unknown error'}`);

		// Fallback to showing data with zeros for increases
		const currentResults = await fetchDockerHubStats(env);
		return currentResults.map(current => ({
			org: current.org,
			repo: current.repo,
			totalPulls: current.pullCount,
			oneDayPulls: 0,
			sevenDayPulls: 0,
			thirtyDayPulls: 0
		}));
	}
}

// Generate HTML for stats page
function generateStatsHtml(stats: DockerHubCombinedStats[]): string {
	return `
	<!DOCTYPE html>
	<html lang="en">
	<head>
		<meta charset="UTF-8">
		<meta name="viewport" content="width=device-width, initial-scale=1.0">
		<title>Docker Hub Statistics</title>
		<style>
			body {
				font-family: Arial, sans-serif;
				margin: 0;
				padding: 20px;
				background-color: #f5f5f5;
			}
			.container {
				max-width: 1000px;
				margin: 0 auto;
				background-color: white;
				border-radius: 8px;
				box-shadow: 0 2px 10px rgba(0, 0, 0, 0.1);
				padding: 20px;
			}
			h1 {
				color: #333;
				text-align: center;
			}
			table {
				width: 100%;
				border-collapse: collapse;
				margin-top: 20px;
			}
			th, td {
				padding: 12px 15px;
				text-align: left;
				border-bottom: 1px solid #ddd;
			}
			th {
				background-color: #f2f2f2;
				font-weight: bold;
			}
			tr:hover {
				background-color: #f9f9f9;
			}
			.positive {
				color: green;
			}
			.updated-time {
				text-align: center;
				color: #666;
				margin-top: 20px;
				font-size: 14px;
			}
			h2 {
				color: #333;
			}
		</style>
	</head>
	<body>
		<div class="container">
			<h1>Docker Hub Statistics</h1>
			
			<div>
				<h2>Pull Statistics</h2>
				<table>
					<thead>
						<tr>
							<th>Repository</th>
							<th>Total Pulls</th>
							<th>1 Day</th>
							<th>7 Days</th>
							<th>30 Days</th>
						</tr>
					</thead>
					<tbody>
						${stats.map(stat => `
							<tr>
								<td>${stat.org}/${stat.repo}</td>
								<td>${stat.totalPulls.toLocaleString()}</td>
								<td class="positive">+${stat.oneDayPulls.toLocaleString()}</td>
								<td class="positive">+${stat.sevenDayPulls.toLocaleString()}</td>
								<td class="positive">+${stat.thirtyDayPulls.toLocaleString()}</td>
							</tr>
						`).join('')}
					</tbody>
				</table>
			</div>
			
			<p class="updated-time">Last updated: ${new Date().toLocaleString()}</p>
		</div>
	</body>
	</html>
	`;
}
