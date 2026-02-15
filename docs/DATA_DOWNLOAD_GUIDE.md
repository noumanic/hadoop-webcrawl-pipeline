# WET Files Download Guide

This guide explains how to download your WET (Web Extracted Text) files using the provided Jupyter notebook.

---

## Overview

- **Dataset Source**: Common Crawl (https://commoncrawl.org/)
- **File Format**: WET (Web Extracted Text) files
- **Total Available Files**: 35,700 WET files
- **Files to Download**: 100 random files
- **Expected Size**: ~20GB (after extraction)
- **Download Location**: `downloaded_wet_files/` folder

---

## Provided Files

### 1. `download_common_crawl.ipynb`
Jupyter notebook that automates the entire download process:
- Downloads 100 random WET files from Common Crawl
- Extracts compressed `.gz` files
- Deletes compressed versions to save space
- Renames files to `data-{id}` format

### 2. `wet.paths`
Contains paths to 35,700 WET files available for download.
- Each line is a URL path to a WET file
- Files are from Common Crawl's 2015-48 dataset
- Total of 35,700 available files to choose from

---

## Quick Start

### Step 1: Install Required Python Packages

```bash
# In Windows (outside WSL)
pip install jupyter requests tqdm
```

### Step 2: Open Jupyter Notebook

```bash
# Navigate to folder containing the notebook
cd /path/to/assignment/folder

# Start Jupyter
jupyter notebook
```

This will open your browser with Jupyter interface.

### Step 3: Open and Run Notebook

1. Click on `download_common_crawl.ipynb`
2. Run all cells in order (Cell → Run All)
3. Wait for downloads to complete (~1-2 hours for 100 files)

---

## 📝 Notebook Walkthrough

### Cell 1: Import Libraries
```python
import requests
import random
import os
from pathlib import Path
from tqdm import tqdm
```

### Cell 2: Configuration
```python
BASE_URL = "https://data.commoncrawl.org/"
PATHS_FILE = "wet.paths"
DOWNLOAD_DIR = "downloaded_wet_files"
NUM_FILES_TO_DOWNLOAD = 100  # Change to download different number
```

**Important**: You can modify `NUM_FILES_TO_DOWNLOAD` to:
- Download fewer files for testing (e.g., 5 or 10)
- Download exactly 100 for the assignment
- Download more if needed

### Cell 3-6: Download Process
These cells:
1. Create download directory
2. Read available paths from `wet.paths`
3. Randomly select files (using seed for reproducibility)
4. Download selected files with progress bars

### Cell 7-9: Extract and Clean
These cells:
1. Extract `.gz` files
2. Delete compressed versions
3. Rename to `data-{id}` format

### Cell 10: Final Summary
Shows list of downloaded files and total size.

---

## Important Notes

### File Naming Convention
Files are renamed from:
```
CC-MAIN-20151124205404-00156-ip-10-71-132-137.ec2.internal.warc.wet.gz
```
To:
```
data-7296
```

Where `7296` is the line number (index) in `wet.paths` file. This ensures:
- Consistent naming
- Easy tracking of which files you downloaded
- Avoids filename conflicts

### Download Time Estimates
- **Per file**: 1-2 minutes (depends on internet speed)
- **10 files**: 10-20 minutes
- **100 files**: 2-3 hours
- **Total size**: Each file ~200-250MB compressed, ~400-500MB extracted

### Storage Requirements
```
Compressed files:   ~20-25 GB
Extracted files:    ~40-50 GB
After cleanup:      ~20 GB (compressed deleted)
```

Make sure you have at least **50GB free space** before starting.

---

## Customization

### Change Number of Files
In the configuration cell:
```python
NUM_FILES_TO_DOWNLOAD = 10  # Start with 10 for testing
```

### Change Random Seed
To get different random files:
```python
random.seed(42)  # Change 42 to any number or remove for truly random
```

### Change Download Location
```python
DOWNLOAD_DIR = "my_custom_folder"
```

---

## What to Expect

### Successful Download Output
```
Starting download of 100 files...

[1/100] Downloading: CC-MAIN-20151124205404-00156...
✓ Successfully downloaded (92.22 MB)

[2/100] Downloading: CC-MAIN-20151124205404-00245...
✓ Successfully downloaded (87.45 MB)

...

================================================================================
Download Summary:
  Successful: 100/100
  Failed: 0
```

### Extraction Output
```
Extracting downloaded files...

Extracting: CC-MAIN-20151124205404-00156...
✓ Extracted (215.11 MB)

...

================================================================================
Extraction Summary:
  Successful: 100
  Failed: 0
```

### Final File List
```
Final files in download directory:
================================================================================

data-156 (215.11 MB)
  Original: CC-MAIN-20151124205404-00156...
  Path ID: 156

data-245 (201.34 MB)
  Original: CC-MAIN-20151124205404-00245...
  Path ID: 245

...

================================================================================
Total files: 100
Total size: 20458.23 MB (19.97 GB)
```

---

## Troubleshooting

### Issue: Download Fails with Network Error
**Solution**: 
```python
# The notebook will continue with next file
# Failed downloads are reported at the end
# You can re-run the cell to retry failed files
```

### Issue: Out of Disk Space
**Solution**:
```bash
# Check available space
df -h

# Free up space or download to external drive
DOWNLOAD_DIR = "/path/to/external/drive/wet_files"
```

### Issue: Jupyter Kernel Dies During Download
**Solution**:
```python
# Reduce number of files
NUM_FILES_TO_DOWNLOAD = 10

# Or run in smaller batches
# Download 10 at a time, then rename the folder before next batch
```

### Issue: ImportError for tqdm
**Solution**:
```bash
pip install tqdm
# or
conda install tqdm
```

---

## Uploading to HDFS (all ~99 files)

The pipeline processes **every file** in the input directory. Upload all WET files so all ~99 are processed.

### Option 1: Upload script (from project root)
```bash
chmod +x scripts/upload_wet_to_hdfs.sh
./scripts/upload_wet_to_hdfs.sh
# Uploads all data-* from dataset-downloader/downloaded_wet_files/ to /user/$USER/input/wet_files/
# Then run: ./scripts/run_pipeline.sh /user/$USER/input/wet_files /user/$USER/output
```

### Option 2: Transfer to WSL and upload
```bash
# In WSL, from project root
cd /mnt/c/Users/YOUR_USERNAME/path/to/hadoop-webcrawl-pipeline

hadoop fs -mkdir -p /user/$USER/input/wet_files
hadoop fs -put dataset-downloader/downloaded_wet_files/data-* /user/$USER/input/wet_files/

# Verify: should show ~99 files
hadoop fs -ls /user/$USER/input/wet_files/
hadoop fs -du -h /user/$USER/input/wet_files/
```

### Option 3: From Windows path in WSL
```bash
# In WSL
hadoop fs -put /mnt/c/Users/YOUR_USERNAME/Desktop/hadoop-webcrawl-pipeline/dataset-downloader/downloaded_wet_files/data-* /user/$USER/input/wet_files/
```

---

## Next Steps

After downloading files:

1. **Verify Download**
   ```bash
   # Count files
   ls downloaded_wet_files/ | wc -l
   # Should show 100
   
   # Check total size
   du -sh downloaded_wet_files/
   # Should show ~20GB
   ```

2. **Upload to HDFS** (see above)

3. **Run Hadoop Pipeline** (processes all files in the input directory)
   ```bash
   ./scripts/run_pipeline.sh /user/$USER/input/wet_files /user/$USER/output
   ```
   The run script reports how many files were found; ensure it matches your ~99 files.

---

## File Format

### WET File Structure
Each WET file contains:
```
WARC/1.0
WARC-Type: conversion
WARC-Target-URI: http://example.com/page
WARC-Date: 2015-11-24T20:54:06Z
Content-Type: text/plain
Content-Length: 1234

[Extracted plain text from webpage]

WARC/1.0
WARC-Type: conversion
...
[Next webpage text]
```

### Content Characteristics
- Plain text extracted from web pages
- Multiple web pages per file
- WARC headers separate each page
- Text is pre-cleaned (no HTML tags)
- Various languages (mostly English)
- Mix of content types (articles, forums, blogs, etc.)

---

## Backup

After downloading:

1. **Keep a backup**
   ```bash
   # Compress for backup
   tar -czf wet_files_backup.tar.gz downloaded_wet_files/
   
   # Store on external drive or cloud
   ```

2. **Document your downloads**
   Create a file `my_downloaded_files.txt`:
   ```bash
   # In notebook directory
   ls downloaded_wet_files/ > my_downloaded_files.txt
   ```
   
   Include this in your submission to show which files you used.

---

## Dataset Statistics

### Common Crawl 2015-48 Dataset
- **Crawl Date**: November 2015
- **Total WET Files Available**: 35,700
- **Total Compressed Size**: ~7TB
- **Average File Size**: ~200MB compressed, ~400MB extracted
- **Content**: Web pages from millions of domains

### Your Subset (100 files)
- **Compressed Size**: ~20-25GB
- **Extracted Size**: ~20GB (after deleting compressed)
- **Estimated Word Count**: ~2-4 billion words
- **Unique Words**: ~5-10 million
- **Perfect for**: Hadoop MapReduce learning and testing

---

## Warnings

1. **Disk Space**: Ensure 50GB free before starting
2. **Time**: Budget 2-3 hours for 100 file downloads
3. **Internet**: Stable connection required (downloads can resume if interrupted)
4. **Deduplication**: Each student gets random files, yours will differ from others
5. **Academic Integrity**: Use your own downloaded dataset

---

## Resources

- **Common Crawl**: https://commoncrawl.org/
- **WET Format Documentation**: https://commoncrawl.org/the-data/get-started/
- **Dataset Information**: https://commoncrawl.org/2015/12/november-2015-crawl-archive-available/

---

## Pre-Download Checklist

Before running the notebook:

- [ ] Python 3.x installed
- [ ] Jupyter notebook installed
- [ ] Required packages installed (requests, tqdm)
- [ ] At least 50GB free disk space
- [ ] Stable internet connection
- [ ] Both files in same directory (`download_common_crawl.ipynb` and `wet.paths`)
- [ ] Time available (2-3 hours)

---

## Sample Download Session

```
1. Start Jupyter → Open notebook
2. Run cells 1-2 → Setup complete
3. Run cells 3-6 → Downloads start
   [1/100] ████████████ 100% | 96.7MB/96.7MB
   [2/100] ████████████ 100% | 87.5MB/87.5MB
   ...
   [100/100] ████████████ 100% | 92.3MB/92.3MB
   ✓ All downloads complete!
   
4. Run cells 7-9 → Extraction and cleanup
   ✓ Extracted 100 files
   ✓ Deleted 100 compressed files
   ✓ Renamed to data-{id} format
   
5. Run cell 10 → Summary
   Total: 100 files, 19.97 GB
   Ready for Hadoop processing!
```

---

Run the notebook to download WET files. See the main [README](../README.md) for pipeline execution.