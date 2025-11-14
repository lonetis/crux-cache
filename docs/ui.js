/**
 * UI Controller for CrUX Dataset Website
 *
 * Handles all user interface interactions and updates
 */

class UIController {
    constructor() {
        this.downloaders = {};
        this.manifests = {};
        this.datasetsInfo = null;
        this.baseUrl = null;
        this.elements = this.getElements();
        this.init();
    }

    /**
     * Get all DOM elements
     */
    getElements() {
        return {
            // Main download section
            downloadBtn: document.getElementById('downloadBtn'),
            latestMonth: document.getElementById('latestMonth'),
            totalOrigins: document.getElementById('totalOrigins'),
            fileSize: document.getElementById('fileSize'),
            progressContainer: document.getElementById('progressContainer'),
            progressFill: document.getElementById('progressFill'),
            progressText: document.getElementById('progressText'),

            // Browse section
            datasetSelect: document.getElementById('datasetSelect'),
            monthSelect: document.getElementById('monthSelect'),
            selectedDatasetInfo: document.getElementById('selectedDatasetInfo'),
            selectedDatasetName: document.getElementById('selectedDatasetName'),
            selectedMonth: document.getElementById('selectedMonth'),
            selectedOrigins: document.getElementById('selectedOrigins'),
            selectedSize: document.getElementById('selectedSize'),
            selectedChunks: document.getElementById('selectedChunks'),
            downloadSelectedBtn: document.getElementById('downloadSelectedBtn'),
            progressContainerSecondary: document.getElementById('progressContainerSecondary'),
            progressFillSecondary: document.getElementById('progressFillSecondary'),
            progressTextSecondary: document.getElementById('progressTextSecondary')
        };
    }

    /**
     * Initialize the UI
     */
    async init() {
        try {
            // Get base URL
            const tempDownloader = new StreamingDownloader('global');
            this.baseUrl = tempDownloader.baseUrl;

            // Load datasets manifest
            this.datasetsInfo = await StreamingDownloader.loadDatasetsManifest(this.baseUrl);

            // Check if any datasets exist
            if (!this.datasetsInfo.datasets || this.datasetsInfo.datasets.length === 0) {
                this.showError('No datasets available yet');
                this.elements.latestMonth.textContent = 'Pending';
                this.elements.totalOrigins.textContent = 'N/A';
                this.elements.fileSize.textContent = 'N/A';

                // Disable browse section
                this.elements.datasetSelect.disabled = true;
                this.elements.downloadSelectedBtn.disabled = true;
                this.elements.downloadSelectedBtn.querySelector('.btn-text').textContent = 'No Datasets Available';
                return;
            }

            // Create downloaders for all available datasets
            for (const dataset of this.datasetsInfo.datasets) {
                this.downloaders[dataset.id] = new StreamingDownloader(dataset.id);
            }

            // Load all manifests
            await this.loadAllManifests();

            // Update main download section with latest global
            this.updateDatasetInfo();

            // Setup browse section
            this.setupBrowseSection();

            // Check if data is available
            const globalDownloader = this.downloaders['global'];
            if (!globalDownloader) {
                this.showError('Global dataset not available');
                return;
            }

            const latestMonth = globalDownloader.getLatestMonth();
            if (!latestMonth) {
                this.elements.downloadBtn.disabled = true;
                this.elements.downloadBtn.querySelector('.btn-text').textContent = 'No Data Available Yet';
                this.elements.latestMonth.textContent = 'Pending';
                this.elements.totalOrigins.textContent = 'N/A';
                this.elements.fileSize.textContent = 'N/A';
                return;
            }

            // Enable main download button
            this.elements.downloadBtn.disabled = false;
            this.elements.downloadBtn.querySelector('.btn-text').textContent = 'Download Latest Global Dataset';

            // Add click handler for main download
            this.elements.downloadBtn.addEventListener('click', () => this.startDownload());

        } catch (error) {
            console.error('Initialization error:', error);
            this.showError('Failed to load dataset information. Please try again later.');
        }
    }

    /**
     * Load all dataset manifests
     */
    async loadAllManifests() {
        const datasetIds = Object.keys(this.downloaders);

        await Promise.all(datasetIds.map(async (datasetId) => {
            try {
                const manifest = await this.downloaders[datasetId].loadManifest();
                this.manifests[datasetId] = manifest;
            } catch (error) {
                console.warn(`Failed to load manifest for ${datasetId}:`, error);
                this.manifests[datasetId] = null;
            }
        }));
    }

    /**
     * Setup browse section with event listeners
     */
    setupBrowseSection() {
        // Populate dataset dropdown
        this.populateDatasetSelector();

        // Dataset selection change
        this.elements.datasetSelect.addEventListener('change', () => {
            this.onDatasetChange();
        });

        // Month selection change
        this.elements.monthSelect.addEventListener('change', () => {
            this.onMonthChange();
        });

        // Download selected button
        this.elements.downloadSelectedBtn.addEventListener('click', () => {
            this.startSelectedDownload();
        });

        // Load initial dataset (global or first available)
        this.onDatasetChange();
    }

    /**
     * Populate dataset selector dropdown
     */
    populateDatasetSelector() {
        this.elements.datasetSelect.innerHTML = '';

        if (!this.datasetsInfo.datasets || this.datasetsInfo.datasets.length === 0) {
            const option = document.createElement('option');
            option.textContent = 'No datasets available';
            this.elements.datasetSelect.appendChild(option);
            this.elements.datasetSelect.disabled = true;
            return;
        }

        for (const dataset of this.datasetsInfo.datasets) {
            const option = document.createElement('option');
            option.value = dataset.id;
            option.textContent = dataset.name.replace('Cached Chrome User Experience Report - ', '');
            this.elements.datasetSelect.appendChild(option);
        }

        // Set global as default if available
        if (this.datasetsInfo.datasets.find(d => d.id === 'global')) {
            this.elements.datasetSelect.value = 'global';
        }
    }

    /**
     * Handle dataset selection change
     */
    onDatasetChange() {
        const selectedDataset = this.elements.datasetSelect.value;
        const manifest = this.manifests[selectedDataset];

        if (!manifest) {
            this.elements.monthSelect.disabled = true;
            this.elements.monthSelect.innerHTML = '<option>No data available</option>';
            this.elements.selectedDatasetInfo.style.display = 'none';
            this.elements.downloadSelectedBtn.disabled = true;
            this.elements.downloadSelectedBtn.querySelector('.btn-text').textContent = 'No Data Available';
            return;
        }

        // Populate month dropdown
        const months = Object.keys(manifest.months).sort().reverse();
        this.elements.monthSelect.innerHTML = '';

        months.forEach(yyyymm => {
            const monthData = manifest.months[yyyymm];
            const year = monthData.year;
            const month = monthData.month;
            const monthName = new Date(year, month - 1).toLocaleString('en', { month: 'long' });

            const option = document.createElement('option');
            option.value = yyyymm;
            option.textContent = `${monthName} ${year}`;
            this.elements.monthSelect.appendChild(option);
        });

        this.elements.monthSelect.disabled = false;

        // Trigger month change to update info
        this.onMonthChange();
    }

    /**
     * Handle month selection change
     */
    onMonthChange() {
        const selectedDataset = this.elements.datasetSelect.value;
        const selectedYyyyMm = this.elements.monthSelect.value;
        const manifest = this.manifests[selectedDataset];

        if (!manifest || !selectedYyyyMm) {
            return;
        }

        const monthData = manifest.months[selectedYyyyMm];

        if (!monthData) {
            return;
        }

        // Get dataset display name from datasets info
        const datasetInfo = this.datasetsInfo.datasets.find(d => d.id === selectedDataset);
        const datasetDisplayName = datasetInfo
            ? datasetInfo.name.replace('Cached Chrome User Experience Report - ', '')
            : selectedDataset;

        const year = monthData.year;
        const month = monthData.month;
        const monthName = new Date(year, month - 1).toLocaleString('en', { month: 'long' });

        this.elements.selectedDatasetName.textContent = datasetDisplayName;
        this.elements.selectedMonth.textContent = `${monthName} ${year}`;
        this.elements.selectedOrigins.textContent = this.formatNumber(monthData.origins || 0);

        const sizeGB = monthData.total_size / (1024 ** 3);
        this.elements.selectedSize.textContent = `${sizeGB.toFixed(2)} GB`;
        this.elements.selectedChunks.textContent = monthData.total_chunks;

        // Show info section
        this.elements.selectedDatasetInfo.style.display = 'block';

        // Enable download button
        this.elements.downloadSelectedBtn.disabled = false;
        this.elements.downloadSelectedBtn.querySelector('.btn-text').textContent = 'Download Selected Dataset';
    }

    /**
     * Update main dataset information (latest global)
     */
    updateDatasetInfo() {
        const manifest = this.manifests.global;
        const latestMonth = this.downloaders.global.getLatestMonth();

        if (!latestMonth) {
            return;
        }

        // Format month display
        const year = latestMonth.year;
        const month = latestMonth.month;
        const monthName = new Date(year, month - 1).toLocaleString('en', { month: 'long' });
        this.elements.latestMonth.textContent = `${monthName} ${year}`;

        // Total origins
        const origins = latestMonth.origins || 0;
        this.elements.totalOrigins.textContent = this.formatNumber(origins);

        // File size
        const sizeGB = latestMonth.total_size / (1024 ** 3);
        this.elements.fileSize.textContent = `${sizeGB.toFixed(2)} GB`;
    }

    /**
     * Start the main download process (latest global)
     */
    async startDownload() {
        const latestYyyyMm = this.manifests.global.summary.latest_month;

        // Update UI state
        this.elements.downloadBtn.disabled = true;
        this.elements.progressContainer.style.display = 'block';
        this.elements.progressFill.style.width = '0%';
        this.elements.progressText.textContent = 'Initializing download...';

        try {
            await this.downloaders.global.downloadMonth(latestYyyyMm, (progress) => {
                this.updateProgress(progress, 'main');
            });

            // Success
            this.elements.progressText.textContent = '✓ Download complete!';
            this.elements.progressFill.style.width = '100%';

            // Reset after delay
            setTimeout(() => {
                this.resetDownloadUI('main');
            }, 3000);

        } catch (error) {
            console.error('Download error:', error);
            this.handleDownloadError(error, 'main');
        }
    }

    /**
     * Start download for selected dataset
     */
    async startSelectedDownload() {
        const selectedDataset = this.elements.datasetSelect.value;
        const selectedYyyyMm = this.elements.monthSelect.value;

        // Update UI state
        this.elements.downloadSelectedBtn.disabled = true;
        this.elements.progressContainerSecondary.style.display = 'block';
        this.elements.progressFillSecondary.style.width = '0%';
        this.elements.progressTextSecondary.textContent = 'Initializing download...';

        try {
            await this.downloaders[selectedDataset].downloadMonth(selectedYyyyMm, (progress) => {
                this.updateProgress(progress, 'secondary');
            });

            // Success
            this.elements.progressTextSecondary.textContent = '✓ Download complete!';
            this.elements.progressFillSecondary.style.width = '100%';

            // Reset after delay
            setTimeout(() => {
                this.resetDownloadUI('secondary');
            }, 3000);

        } catch (error) {
            console.error('Download error:', error);
            this.handleDownloadError(error, 'secondary');
        }
    }

    /**
     * Update progress indicators
     */
    updateProgress(progress, section) {
        const percentage = (progress.bytesDownloaded / progress.totalBytes) * 100;

        const fillEl = section === 'main' ? this.elements.progressFill : this.elements.progressFillSecondary;
        const textEl = section === 'main' ? this.elements.progressText : this.elements.progressTextSecondary;

        fillEl.style.width = `${percentage.toFixed(1)}%`;

        if (progress.type === 'chunk') {
            const chunkNum = progress.chunkIndex + 1;
            textEl.textContent = `Downloading chunk ${chunkNum} of ${progress.totalChunks}...`;
        } else if (progress.type === 'progress') {
            const mbDownloaded = progress.bytesDownloaded / (1024 ** 2);
            const mbTotal = progress.totalBytes / (1024 ** 2);
            textEl.textContent = `${mbDownloaded.toFixed(0)} MB of ${mbTotal.toFixed(0)} MB (${percentage.toFixed(1)}%)`;
        } else if (progress.type === 'complete') {
            textEl.textContent = 'Finalizing download...';
        }
    }

    /**
     * Handle download error
     */
    handleDownloadError(error, section) {
        const textEl = section === 'main' ? this.elements.progressText : this.elements.progressTextSecondary;

        if (error.name === 'AbortError') {
            textEl.textContent = 'Download cancelled';
        } else {
            textEl.textContent = '✗ Download failed. Please try again.';
        }

        setTimeout(() => {
            this.resetDownloadUI(section);
        }, 3000);
    }

    /**
     * Reset download UI to initial state
     */
    resetDownloadUI(section) {
        if (section === 'main') {
            this.elements.downloadBtn.disabled = false;
            this.elements.progressContainer.style.display = 'none';
            this.elements.progressFill.style.width = '0%';
        } else {
            this.elements.downloadSelectedBtn.disabled = false;
            this.elements.progressContainerSecondary.style.display = 'none';
            this.elements.progressFillSecondary.style.width = '0%';
        }
    }

    /**
     * Show error message
     */
    showError(message) {
        this.elements.downloadBtn.disabled = true;
        this.elements.downloadBtn.querySelector('.btn-text').textContent = message;
    }

    /**
     * Format large numbers with commas
     */
    formatNumber(num) {
        return num.toLocaleString('en-US');
    }
}

// Initialize when DOM is ready
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => {
        new UIController();
    });
} else {
    new UIController();
}
