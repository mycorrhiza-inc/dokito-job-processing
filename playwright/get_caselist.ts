import { Browser, chromium } from "playwright";
import { RawGenericDocket } from "./types";
import * as fs from "fs";

class NyPucCaseListScraper {
  state = "ny";
  jurisdiction_name = "ny_puc";
  context: any;
  browser: Browser;
  rootPage: any;
  pageCache: Record<string, any> = {};

  constructor(page: any, context: any, browser: Browser) {
    this.rootPage = page;
    this.context = context;
    this.browser = browser;
  }

  async newWindow(): Promise<{ context: any; page: any }> {
    const context = await this.browser.newContext();
    const page = await context.newPage();
    return { context, page };
  }

  async waitForLoadingSpinner(page: any, timeout: number = 0): Promise<void> {
    await page.waitForFunction(
      () => {
        const loadingSpinner = document.querySelector('#GridPlaceHolder_UpdateProgress1');
        return !loadingSpinner || window.getComputedStyle(loadingSpinner).display === 'none';
      },
      { timeout }
    );
  }

  async getPage(url: string): Promise<any> {
    const cached = this.pageCache[url];
    if (cached !== undefined) {
      return cached;
    }

    let context = null;
    let retries = 3;

    for (let attempt = 1; attempt <= retries; attempt++) {
      try {
        const windowContext = await this.newWindow();
        context = windowContext.context;
        const page = windowContext.page;

        await page.waitForTimeout(100);
        await page.goto(url, {
          waitUntil: 'domcontentloaded',
          timeout: 180000
        });
        await page.waitForLoadState("networkidle", { timeout: 180000 });
        await this.waitForLoadingSpinner(page);

        console.log(`Getting page content at ${url}...`);
        const cheerio = await import("cheerio");
        const html = await page.content();
        const $ = cheerio.load(html);

        this.pageCache[url] = $;
        await context.close();
        return $;
      } catch (error: any) {
        console.error(`Attempt ${attempt}/${retries} failed for ${url}:`, error.message);
        if (context) {
          try {
            await context.close();
          } catch (closeError) {
            console.error('Error closing context:', closeError);
          }
        }

        if (attempt === retries) {
          throw error;
        }

        await new Promise(resolve => setTimeout(resolve, 1000 * attempt));
      }
    }
  }

  async getGeneralRows($: any) {
    return $("#tblSearchedMatterExternal > tbody tr");
  }

  async getCasesAt(url: string): Promise<Partial<RawGenericDocket>[]> {
    console.log(`Navigating to ${url}`);
    const $ = await this.getPage(url);

    const cases: Partial<RawGenericDocket>[] = [];

    console.log("Extracting industry affected...");
    let industry_affected = $("#GridPlaceHolder_lblSearchCriteriaValue")
      .text()
      .trim();

    if (industry_affected.startsWith("Industry Affected:")) {
      industry_affected = industry_affected
        .replace("Industry Affected:", "")
        .trim();
    } else {
      industry_affected = "Unknown";
    }

    console.log("Extracting rows from the table...");
    const rows = await this.getGeneralRows($);
    console.log(`Found ${rows.length} rows.`);

    rows.each((i: number, row: any) => {
      const cells = $(row).find("td");
      if (cells.length >= 6) {
        const case_govid = $(cells[0]).find("a").text();
        const case_url = `https://documents.dps.ny.gov/public/MatterManagement/CaseMaster.aspx?MatterCaseNo=${case_govid}`;
        const matter_type = $(cells[1]).text();
        const matter_subtype = $(cells[2]).text();
        const opened_date = $(cells[3]).text();
        const case_name = $(cells[4]).text();
        const petitioner = $(cells[5]).text();

        const caseData: Partial<RawGenericDocket> = {
          case_govid,
          case_url,
          case_name,
          opened_date: new Date(opened_date).toISOString(),
          case_type: matter_type,
          case_subtype: matter_subtype,
          petitioner,
          industry: industry_affected,
        };
        cases.push(caseData);
      }
    });

    return cases;
  }

  async getAllCaseList(): Promise<Partial<RawGenericDocket>[]> {
    const industry_numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    const promises = industry_numbers.map((industry_number) => {
      console.log(`Processing industry index: ${industry_number}`);
      const industry_url = `https://documents.dps.ny.gov/public/Common/SearchResults.aspx?MC=1&IA=${industry_number}`;
      return this.getCasesAt(industry_url);
    });
    const results = await Promise.all(promises);
    return results.flat();
  }
}

function parseArguments(): { outFile?: string; headed: boolean } {
  const args = process.argv.slice(2);
  let outFile: string | undefined;
  let headed = false;

  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (arg === "-o" || arg === "--outfile") {
      outFile = args[++i];
    } else if (arg === "--headed") {
      headed = true;
    }
  }

  return { outFile, headed };
}

async function main() {
  let browser: Browser | null = null;
  try {
    const { outFile, headed } = parseArguments();

    browser = await chromium.launch({ headless: !headed });
    const context = await browser.newContext();
    const rootpage = await context.newPage();
    const scraper = new NyPucCaseListScraper(rootpage, context, browser);

    console.log("Fetching NY PUC caselist...");
    const cases = await scraper.getAllCaseList();
    console.log(`Total cases: ${cases.length}`);

    const jsonOutput = JSON.stringify(cases, null, 2);

    if (outFile) {
      // Write to file
      fs.writeFileSync(outFile, jsonOutput, "utf-8");
      console.log(`✅ Successfully saved ${cases.length} cases to ${outFile}`);
    } else {
      // Output to stdout
      console.log("\n=== CASELIST JSON ===");
      console.log(jsonOutput);
    }
  } catch (error) {
    console.error("Scraper failed:", error);
    process.exit(1);
  } finally {
    if (browser) {
      await browser.close();
    }
  }
}

main();
