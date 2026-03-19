# Apartment Scraper — Setup Guide

## What You Need

The scraper uses **Playwright**, which is a tool that launches a real (invisible) web browser on your computer. This browser can click buttons, wait for content to load, and read the page — just like a person would. That's how it gets past the floor tabs that regular web scraping can't handle.

---

## Step-by-Step Setup

### 1. Install Python (if you don't have it)

Open **Terminal** (Mac) or **Command Prompt** (Windows) and type:

```
python --version
```

If you see something like `Python 3.10.x` or higher, you're good. If not, download Python from [python.org](https://python.org/downloads/).

---

### 2. Install the Required Packages

In your terminal, run these commands **one at a time**:

```
pip install playwright
```

```
pip install beautifulsoup4
```

```
pip install lxml
```

Then install the Chromium browser that Playwright will use:

```
playwright install chromium
```

> **Note:** If `pip` doesn't work, try `pip3` instead. If you get a permissions error, add `--user` at the end (e.g., `pip install playwright --user`).

---

### 3. Download the Scraper Files

Make sure these files are in the **same folder** on your computer:

- `apartment_scraper.py` — the main scraper engine
- `test_rosemont.py` — the quick test script

---

### 4. Run the Test

In your terminal, navigate to the folder where you saved the files:

```
cd /path/to/your/folder
```

Then run:

```
python test_rosemont.py
```

This will:
1. Check that everything is installed correctly
2. Open an invisible browser
3. Go to the Rosemont Berkeley Lake floor plans page
4. Click through floor tabs 0, 1, and 2
5. Extract all available units
6. Save the results to `rosemont_results.json`
7. Print a summary in your terminal

---

## Running on Other Properties

### Basic usage (auto-detect tabs):

```
python apartment_scraper.py --url "https://propertywebsite.com/floor-plans/"
```

### When you know the tab labels:

```
python apartment_scraper.py --url "https://propertywebsite.com/floor-plans/" --tabs "0,1,2" --tab-type floor
```

### With bedroom tabs instead of floor tabs:

```
python apartment_scraper.py --url "https://propertywebsite.com/floor-plans/" --tabs "1 Bed,2 Bed,3 Bed" --tab-type bedroom
```

### Watch the browser (debugging mode):

```
python apartment_scraper.py --url "https://propertywebsite.com/floor-plans/" --tabs "0,1,2" --headed
```

This opens a **visible** browser window so you can see exactly what the scraper is clicking and reading. Very useful for figuring out why units aren't being captured.

### Save as CSV instead of JSON:

```
python apartment_scraper.py --url "https://..." --output results.csv --format csv
```

### Batch scrape multiple properties:

Create a text file (`urls.txt`) with one URL per line:

```
https://rosemontberkeleylake.com/floor-plans/
https://anotherproperty.com/availability/
https://thirdproperty.com/floor-plans/
```

Then run:

```
python apartment_scraper.py --batch urls.txt --output all_results.json
```

---

## Troubleshooting

### "No module named playwright"
Run: `pip install playwright` (or `pip3 install playwright`)

### "Executable doesn't exist" or "Browser not found"
Run: `playwright install chromium`

### "Permission denied"
Try adding `--user` to the pip commands, or use `sudo pip install ...` on Mac/Linux.

### Scraper runs but finds 0 units
1. Run with `--headed` to see the browser
2. Check if the page has a cookie consent popup blocking content
3. Check if units are inside an iframe (the scraper checks for this, but some iframes are tricky)
4. Try different `--tabs` values that match what you see on the website
5. The leasing widget might be on a different page — try the direct availability URL

### Scraper finds some units but not all
- Some tabs might have different labels than expected
- Run with `--headed` and watch which tabs it clicks
- Try passing the exact tab text: `--tabs "Floor 0,Floor 1,Floor 2"`

---

## How It Works (For Reference)

1. **Launches a headless Chromium browser** — invisible, but fully functional
2. **Navigates to the URL** and waits for the page to fully load
3. **Listens for API calls** — many sites fetch unit data via background JSON requests, which the scraper intercepts automatically
4. **Clicks "Load More" / "View All" buttons** if they exist
5. **Detects and clicks through tabs** — floor selectors, bedroom filters, etc.
6. **Takes an HTML snapshot after each tab click** — capturing the newly rendered units
7. **Scrolls the page** to trigger any lazy-loaded content
8. **Checks for iframes** — some properties embed their leasing widget in an iframe
9. **Parses everything** — HTML cards, embedded JavaScript data, and intercepted API responses
10. **Deduplicates** and outputs clean JSON or CSV
