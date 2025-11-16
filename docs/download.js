/**
 * Streaming Download Engine for CrUX Dataset
 *
 * This module handles efficient streaming downloads by:
 * 1. Fetching chunks sequentially from GitHub
 * 2. Merging them on-the-fly (removing duplicate headers)
 * 3. Streaming directly to download (zero memory overhead)
 */

class StreamingDownloader {
    constructor(dataset = 'global') {
        this.dataset = dataset;
        this.manifest = null;
        this.isDownloading = false;
        this.abortController = null;

        // GitHub raw content base URL
        // This will be dynamically constructed based on the repo
        this.baseUrl = this.getBaseUrl();
    }

    /**
     * Construct the base URL for raw GitHub content
     */
    getBaseUrl() {
        // Extract from current location or use default
        // For GitHub Pages: https://username.github.io/repo/
        // Raw files: https://raw.githubusercontent.com/username/repo/main/

        const currentHost = window.location.hostname;

        if (currentHost.includes('github.io')) {
            // GitHub Pages - construct raw URL
            const pathParts = window.location.pathname.split('/').filter(p => p);
            const repo = pathParts[0] || 'crux';
            const username = currentHost.split('.')[0];
            return `https://raw.githubusercontent.com/${username}/${repo}/main/`;
        }

        // Local development - use relative path
        return '../';
    }

    /**
     * Load datasets manifest
     */
    static async loadDatasetsManifest(baseUrl) {
        try {
            const manifestUrl = `${baseUrl}data/datasets.json`;
            const response = await fetch(manifestUrl);

            if (!response.ok) {
                throw new Error(`Failed to load datasets manifest: ${response.status}`);
            }

            return await response.json();
        } catch (error) {
            console.error('Error loading datasets manifest:', error);
            throw error;
        }
    }

    /**
     * Load manifest.json from repository
     */
    async loadManifest(dataset = null) {
        try {
            const targetDataset = dataset || this.dataset;
            const manifestUrl = `${this.baseUrl}data/${targetDataset}/manifest.json`;
            const response = await fetch(manifestUrl);

            if (!response.ok) {
                throw new Error(`Failed to load manifest: ${response.status}`);
            }

            const manifest = await response.json();

            // If loading for current instance, store it
            if (!dataset || dataset === this.dataset) {
                this.manifest = manifest;
            }

            return manifest;
        } catch (error) {
            console.error('Error loading manifest:', error);
            throw error;
        }
    }

    /**
     * Get the latest month data from manifest
     */
    getLatestMonth() {
        if (!this.manifest || !this.manifest.summary) {
            return null;
        }

        const latestYyyyMm = this.manifest.summary.latest_month;
        return this.manifest.months[latestYyyyMm];
    }

    /**
     * Create a streaming download for the specified month
     */
    async downloadMonth(yyyymm, onProgress) {
        if (this.isDownloading) {
            throw new Error('Download already in progress');
        }

        const monthData = this.manifest.months[yyyymm];
        if (!monthData) {
            throw new Error(`Month ${yyyymm} not found in manifest`);
        }

        this.isDownloading = true;
        this.abortController = new AbortController();

        try {
            await this.streamDownload(yyyymm, monthData, onProgress);
        } finally {
            this.isDownloading = false;
            this.abortController = null;
        }
    }

    /**
     * Perform the actual streaming download
     */
    async streamDownload(yyyymm, monthData, onProgress) {
        const chunks = monthData.chunks;
        const totalChunks = chunks.length;
        const totalSize = monthData.total_size;
        const dataset = this.dataset; // Capture dataset for filename
        const self = this; // Preserve context for use inside ReadableStream

        // Create a readable stream that will merge all chunks
        const stream = new ReadableStream({
            async start(controller) {
                let bytesDownloaded = 0;

                for (let i = 0; i < totalChunks; i++) {
                    if (self.abortController.signal.aborted) {
                        controller.close();
                        return;
                    }

                    const chunk = chunks[i];
                    const chunkUrl = `${self.baseUrl}data/${self.dataset}/${chunk.filename}`;

                    onProgress({
                        type: 'chunk',
                        chunkIndex: i,
                        totalChunks: totalChunks,
                        bytesDownloaded: bytesDownloaded,
                        totalBytes: totalSize
                    });

                    try {
                        const response = await fetch(chunkUrl, {
                            signal: self.abortController.signal
                        });

                        if (!response.ok) {
                            throw new Error(`Failed to fetch chunk ${i + 1}: ${response.status}`);
                        }

                        const reader = response.body.getReader();
                        const decoder = new TextDecoder();
                        let buffer = '';

                        while (true) {
                            const { done, value } = await reader.read();

                            if (done) break;

                            bytesDownloaded += value.length;
                            buffer += decoder.decode(value, { stream: true });

                            // Process complete lines
                            const lines = buffer.split('\n');
                            buffer = lines.pop(); // Keep incomplete line in buffer

                            for (const line of lines) {
                                // Enqueue line with newline
                                controller.enqueue(new TextEncoder().encode(line + '\n'));
                            }

                            onProgress({
                                type: 'progress',
                                chunkIndex: i,
                                totalChunks: totalChunks,
                                bytesDownloaded: bytesDownloaded,
                                totalBytes: totalSize
                            });
                        }

                        // Handle remaining buffer
                        if (buffer) {
                            controller.enqueue(new TextEncoder().encode(buffer + '\n'));
                        }

                    } catch (error) {
                        controller.error(error);
                        throw error;
                    }
                }

                controller.close();
                onProgress({
                    type: 'complete',
                    bytesDownloaded: bytesDownloaded,
                    totalBytes: totalSize
                });
            }
        });

        // Create download link and trigger
        const blob = await new Response(stream).blob();
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `crux_${dataset}_${yyyymm}.csv`;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
    }

    /**
     * Cancel ongoing download
     */
    cancelDownload() {
        if (this.abortController) {
            this.abortController.abort();
        }
    }
}

// Export for use in ui.js
window.StreamingDownloader = StreamingDownloader;
