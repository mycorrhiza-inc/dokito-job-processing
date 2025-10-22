import { Scraper } from "./pipeline";
import {
  RawGenericDocket,
  RawGenericFiling,
  RawGenericParty,
  RawArtificalPersonType,
} from "./types";
import { Page } from "playwright";
import { Browser, chromium } from "playwright";
import { runCli } from "./cli_runner";
import * as cheerio from "cheerio";
import * as fs from "fs";
import * as fsPromises from "fs/promises";
import * as path from "path";
import * as crypto from "crypto";

enum ScrapingMode {
  METADATA = "meta",
  filing = "filing",
  PARTIES = "parties",
  ALL = "all",
}

interface ScrapingOptions {
  mode: ScrapingMode;
  govIds?: string[];
  dateString?: string;
  beginDate?: string;
  endDate?: string;
  fromFile?: string;
  outFile?: string;
  headed?: boolean;
  missing?: boolean;
  todayFilings?: boolean;
  intermediateDir?: string;
}

// class NyPucScraper implements Scraper {
class NyPucScraper {
  state = "ny";
  jurisdiction_name = "ny_puc";
  context: any;
  browser: Browser;
  rootPage: Page;
  max_tabs: number = 10;
  max_concurrent_browsers: number = 4;
  pageCache: Record<string, any> = {};
  urlTable: Record<string, string> = {};
  baseDirectory: string;

  constructor(
    page: Page,
    context: any,
    browser: Browser,
    baseDirectory: string,
  ) {
    this.rootPage = page;
    this.context = context;
    this.browser = browser;
    this.baseDirectory = baseDirectory;
  }

  pages(): any {
    return this.context.pages;
  }

  async newTab(): Promise<any> {
    const page = await this.context.newPage();
    return page;
  }

  async newWindow(): Promise<{ context: any; page: any }> {
    const context = await this.browser.newContext();
    const page = await context.newPage();
    return { context, page };
  }

  async waitForLoadingSpinner(page: Page, timeout: number = 0): Promise<void> {
    await page.waitForFunction(
      () => {
        const loadingSpinner = document.querySelector(
          "#GridPlaceHolder_UpdateProgress1",
        );
        return (
          !loadingSpinner ||
          window.getComputedStyle(loadingSpinner).display === "none"
        );
      },
      { timeout },
    );
  }

  private calculateBlake2Hash(content: string): string {
    return crypto.createHash("blake2b512").update(content).digest("hex");
  }

  private async updateMetadataJson(
    directory: string,
    filename: string,
    metadata: {
      filename: string;
      blake2_hash: string;
      url: string;
      stage: string;
      saved_at: string;
      file_size: number;
      parsed_data?: any;
    },
  ): Promise<void> {
    const metadataPath = path.join(directory, "metadata.json");

    let existingMetadata: Record<string, any> = {};
    try {
      const content = await fsPromises.readFile(metadataPath, "utf-8");
      existingMetadata = JSON.parse(content);
    } catch (error) {
      // File doesn't exist or is invalid, start with empty object
    }

    existingMetadata[filename] = metadata;

    await fsPromises.writeFile(
      metadataPath,
      JSON.stringify(existingMetadata, null, 2),
      "utf-8",
    );
  }

  private async findCachedParsedData(
    url: string,
    stage: string,
    currentHash: string,
  ): Promise<any | null> {
    try {
      // Skip if no base directory configured
      if (!this.baseDirectory) {
        return null;
      }

      // Parse URL to build directory path
      const urlObj = new URL(url);
      const hostname = urlObj.hostname;
      const pathname = urlObj.pathname;

      const pathParts = pathname.split("/").filter((part) => part.length > 0);
      pathParts.pop(); // Remove filename
      const directoryPath = pathParts.join("/");

      const fullDirectory = path.join(
        this.baseDirectory,
        "raw",
        "ny",
        "puc",
        hostname,
        directoryPath,
      );

      const metadataPath = path.join(fullDirectory, "metadata.json");

      // Try to read metadata.json
      const content = await fsPromises.readFile(metadataPath, "utf-8");
      const metadata: Record<string, any> = JSON.parse(content);

      // Search for entries matching URL, stage, and hash
      for (const [filename, entry] of Object.entries(metadata)) {
        if (
          entry.url === url &&
          entry.stage === stage &&
          entry.blake2_hash === currentHash &&
          entry.parsed_data !== undefined
        ) {
          console.error(
            `Cache HIT for ${stage} at ${url} (hash: ${currentHash.substring(0, 16)}...)`,
          );
          return entry.parsed_data;
        }
      }

      console.error(
        `Cache MISS for ${stage} at ${url} (hash: ${currentHash.substring(0, 16)}...)`,
      );
      return null;
    } catch (error) {
      // File doesn't exist or other error - cache miss
      console.error(`Cache MISS for ${stage} at ${url} (no metadata file)`);
      return null;
    }
  }

  private async saveParsedDataToCache(
    url: string,
    stage: string,
    hash: string,
    parsedData: any,
  ): Promise<void> {
    try {
      // Skip if no base directory configured
      if (!this.baseDirectory) {
        return;
      }

      // Parse URL to build directory path
      const urlObj = new URL(url);
      const hostname = urlObj.hostname;
      const pathname = urlObj.pathname;

      const pathParts = pathname.split("/").filter((part) => part.length > 0);
      pathParts.pop(); // Remove filename
      const directoryPath = pathParts.join("/");

      const fullDirectory = path.join(
        this.baseDirectory,
        "raw",
        "ny",
        "puc",
        hostname,
        directoryPath,
      );

      const metadataPath = path.join(fullDirectory, "metadata.json");

      // Read existing metadata
      let metadata: Record<string, any> = {};
      try {
        const content = await fsPromises.readFile(metadataPath, "utf-8");
        metadata = JSON.parse(content);
      } catch (error) {
        // File doesn't exist yet, that's okay
      }

      // Find the most recent entry matching URL, stage, and hash
      let targetEntry: any = null;
      let targetFilename: string | null = null;
      let latestTimestamp: string | null = null;

      for (const [filename, entry] of Object.entries(metadata)) {
        if (
          entry.url === url &&
          entry.stage === stage &&
          entry.blake2_hash === hash
        ) {
          if (!latestTimestamp || entry.saved_at > latestTimestamp) {
            targetEntry = entry;
            targetFilename = filename;
            latestTimestamp = entry.saved_at;
          }
        }
      }

      // Update the entry with parsed data
      if (targetFilename && targetEntry) {
        targetEntry.parsed_data = parsedData;
        metadata[targetFilename] = targetEntry;

        await fsPromises.writeFile(
          metadataPath,
          JSON.stringify(metadata, null, 2),
          "utf-8",
        );

        console.error(`Saved parsed data to cache for ${stage} at ${url}`);
      } else {
        console.warn(
          `Could not find metadata entry to update for ${stage} at ${url}`,
        );
      }
    } catch (error) {
      console.error(`Failed to save parsed data to cache for ${url}:`, error);
    }
  }

  private async saveHtmlSnapshot(
    html: string,
    url: string,
    stage: string,
  ): Promise<void> {
    try {
      // Skip if no base directory configured
      if (!this.baseDirectory) {
        return;
      }

      // Parse URL
      const urlObj = new URL(url);
      const hostname = urlObj.hostname;
      const pathname = urlObj.pathname;
      const query = urlObj.search;

      // Split pathname to get directory and filename
      const pathParts = pathname.split("/").filter((part) => part.length > 0);
      const filename = pathParts.pop() || "index.html";
      const directoryPath = pathParts.join("/");

      // Build full directory path
      const fullDirectory = path.join(
        this.baseDirectory,
        "raw",
        "ny",
        "puc",
        hostname,
        directoryPath,
      );

      // Create directory if it doesn't exist
      await fsPromises.mkdir(fullDirectory, { recursive: true });

      // Escape forward slashes in query
      const escapedQuery = query.replace(/\//g, "\\/");

      // Build filename with timestamp and stage
      const timestamp = new Date().toISOString();
      const fullFilename = `${filename}\\${escapedQuery}--${timestamp}--${stage}.html`;

      // Calculate hash
      const blake2Hash = this.calculateBlake2Hash(html);

      // Write HTML file
      const fullPath = path.join(fullDirectory, fullFilename);
      await fsPromises.writeFile(fullPath, html, "utf-8");

      // Update metadata.json
      await this.updateMetadataJson(fullDirectory, fullFilename, {
        filename: fullFilename,
        blake2_hash: blake2Hash,
        url: url,
        stage: stage,
        saved_at: timestamp,
        file_size: Buffer.byteLength(html, "utf-8"),
      });

      console.error(`Saved HTML snapshot: ${fullPath}`);
    } catch (error) {
      console.error(`Failed to save HTML snapshot for ${url}:`, error);
    }
  }

  async processTasksWithQueue<T, R>(
    tasks: T[],
    taskProcessor: (task: T) => Promise<R>,
    maxConcurrent: number = this.max_concurrent_browsers,
  ): Promise<R[]> {
    return new Promise((resolve, reject) => {
      const results: R[] = [];
      const errors: Error[] = [];
      let currentIndex = 0;
      let completed = 0;
      let running = 0;

      const processNext = async () => {
        if (currentIndex >= tasks.length) return;
        if (running >= maxConcurrent) return;

        const taskIndex = currentIndex++;
        const task = tasks[taskIndex];
        running++;

        console.error(
          `Starting task ${taskIndex + 1}/${tasks.length} (${running} running, max: ${maxConcurrent})`,
        );

        try {
          const result = await taskProcessor(task);
          results[taskIndex] = result;
        } catch (error) {
          console.error(`Task ${taskIndex + 1} failed:`, error);
          errors.push(error as Error);
          results[taskIndex] = null as any; // Placeholder for failed result
        } finally {
          running--;
          completed++;
          console.error(
            `Completed task ${taskIndex + 1}/${tasks.length} (${running} still running)`,
          );

          if (completed === tasks.length) {
            if (errors.length > 0) {
              console.warn(
                `${errors.length} tasks failed, but continuing with successful results`,
              );
            }
            resolve(results.filter((r) => r !== null));
          } else {
            // Start more tasks
            while (running < maxConcurrent && currentIndex < tasks.length) {
              processNext();
            }
          }
        }
      };

      // Start initial batch of tasks
      const initialBatch = Math.min(maxConcurrent, tasks.length);
      for (let i = 0; i < initialBatch; i++) {
        processNext();
      }
    });
  }

  async getPage(url: string, stage?: string): Promise<any> {
    this.urlTable[url] = url;
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

        // Add a small delay to ensure context is fully initialized
        await page.waitForTimeout(100);

        await page.goto(url, {
          waitUntil: "domcontentloaded",
          timeout: 180000,
        });
        await page.waitForLoadState("networkidle", { timeout: 180000 });

        // Wait for the loading spinner to disappear
        await this.waitForLoadingSpinner(page);

        console.error(`Getting page content at ${url}...`);
        const html = await page.content();

        // Save HTML snapshot if stage is provided
        if (stage) {
          await this.saveHtmlSnapshot(html, url, stage);
        }

        const $ = cheerio.load(html);
        this.pageCache[url] = $;
        page.url();
        await context.close();
        return $;
      } catch (error) {
        console.error(
          `Attempt ${attempt}/${retries} failed for ${url}:`,
          error.message,
        );
        if (context) {
          try {
            await context.close();
          } catch (closeError) {
            console.error("Error closing context:", closeError.message);
          }
        }

        if (attempt === retries) {
          throw error;
        }

        // Wait before retry
        await new Promise((resolve) => setTimeout(resolve, 1000 * attempt));
      }
    }
  }

  async getDateCases(dateString: string): Promise<Partial<RawGenericDocket>[]> {
    // eg dateString = 09/09/2025
    const url = `https://documents.dps.ny.gov/public/Common/SearchResults.aspx?MC=1&IA=&MT=&MST=&CN=&SDT=${dateString}&SDF=${dateString}&C=&M=&CO=0`;
    console.error(`getting cases for date ${dateString}`);
    const cases: Partial<RawGenericDocket>[] = await this.getCasesAt(url);
    return cases;
  }

  async getGeneralRows($: any) {
    return $("#tblSearchedMatterExternal > tbody tr");
  }

  async getCasesAt(url: string): Promise<Partial<RawGenericDocket>[]> {
    console.error(`Navigating to ${url}`);
    const $ = await this.getPage(url, "search-results");

    // Calculate hash for cache check
    const html = $.html();
    const htmlHash = this.calculateBlake2Hash(html);

    // Check cache first
    const cachedData = await this.findCachedParsedData(
      url,
      "search-results",
      htmlHash,
    );
    if (cachedData) {
      return cachedData;
    }

    const cases: Partial<RawGenericDocket>[] = [];

    console.error("Extracting industry affected...");
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

    console.error("Extracting rows from the table...");
    const rows = await this.getGeneralRows($);
    console.error(`Found ${rows.length} rows.`);

    rows.each((i, row) => {
      const cells = $(row).find("td");
      if (cells.length >= 6) {
        console.error("Extracting case data from row...");
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
        console.error(`Successfully extracted case: ${case_govid}`);
      }
    });

    // Save parsed data to cache
    await this.saveParsedDataToCache(url, "search-results", htmlHash, cases);

    return cases;
  }

  // TODO: This should probably get filtered out to the general task running layer.
  async filterOutExisting(
    cases: Partial<RawGenericDocket>[],
  ): Promise<Partial<RawGenericDocket>[]> {
    const caseDiffUrl =
      "http://localhost:33399/public/caselist/ny/ny_puc/casedata_differential";

    try {
      const response = await fetch(caseDiffUrl, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(cases),
      });

      if (!response.ok) {
        throw new Error(`Request failed with status ${response.status}`);
      }

      const data = await response.json();

      if (!data || !Array.isArray(data.to_process)) {
        throw new Error("Invalid response format: missing 'to_process' array");
      }

      // Assume the backend returns objects in the correct RawGenericDocket shape
      return data.to_process as RawGenericDocket[];
    } catch (err) {
      console.error("filterOutExisting failed:", err);
      return [];
    }
  }

  // gets ALL cases
  async getAllCaseList(): Promise<Partial<RawGenericDocket>[]> {
    const industry_numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    const promises = industry_numbers.map((industry_number) => {
      console.error(`Processing industry index: ${industry_number}`);
      const industry_url = `https://documents.dps.ny.gov/public/Common/SearchResults.aspx?MC=1&IA=${industry_number}`;
      return this.getCasesAt(industry_url);
    });
    const results = await Promise.all(promises);
    return results.flat();
  }
  async getAllMissingCaseList(): Promise<Partial<RawGenericDocket>[]> {
    const cases = await this.getAllCaseList();
    const filtered_cases = await this.filterOutExisting(cases);
    return filtered_cases;
  }

  private async scrapeIndustryAffectedFromFillingsHtml(
    html: string,
  ): Promise<string> {
    const $ = cheerio.load(html);
    const industryElement = $(
      "#GridPlaceHolder_MatterControl1_lblIndustryAffectedValue",
    );

    if (industryElement.length > 0) {
      return industryElement.text().trim();
    }

    return "Unknown";
  }

  private async scrapeMetadataFromCasePageHtml(
    html: string,
    govId: string,
  ): Promise<Partial<RawGenericDocket>> {
    const $ = cheerio.load(html);

    const metadata: Partial<RawGenericDocket> = {
      case_govid: govId,
      case_url: `https://documents.dps.ny.gov/public/MatterManagement/CaseMaster.aspx?MatterCaseNo=${govId}`,
    };

    // So this code should be extracting information from this element: <textarea name="ctl00$GridPlaceHolder$MatterControl1$txtTitleofMatterValue" rows="3" cols="30" readonly="readonly" id="GridPlaceHolder_MatterControl1_txtTitleofMatterValue" class="form_input_box_greyed" aria-labelledby="GridPlaceHolder_MatterControl1_lblTitleofMatter">In the Matter of Staff's Proposal to Examine the Issues Concerning the Cross Connection of Verizon New York Inc. fka New York Telephone Company's House and Riser Cables.</textarea>
    //
    // But instead its pulling information off of this element leading to the wrong name of the case:
    //
    // <span id="GridPlaceHolder_MatterControl1_lblTitleofMatter" class="form_label_head" role="label" style="display:inline-block;width:222px;">
    // <label for="GridPlaceHolder_MatterControl1_txtTitleofMatterValue">Title of Matter/Case:</label></span><span id="GridPlaceHolder_MatterControl1_lblTitleofMatter" class="form_label_head" role="label" style="display:inline-block;width:222px;">
    // <label for="GridPlaceHolder_MatterControl1_txtTitleofMatterValue">Title of Matter/Case:</label></span>
    // Extract case name
    const caseName = $("#GridPlaceHolder_MatterControl1_txtTitleofMatterValue")
      .text()
      .trim();
    if (caseName && caseName != "Title of Matter/Case:") {
      metadata.case_name = caseName;
    }

    // Extract matter type
    const matterType = $("#GridPlaceHolder_MatterControl1_lblMatterTypeValue")
      .text()
      .trim();
    if (matterType) metadata.case_type = matterType;

    // Extract matter subtype
    let matterSubtype = $(
      "#GridPlaceHolder_MatterControl1_lblMatterSubtypeValue",
    )
      .text()
      .trim();

    if (matterSubtype) {
      const prefix = "NEW Case Proceeding:";
      if (matterSubtype.toLowerCase().startsWith(prefix.toLowerCase())) {
        matterSubtype = matterSubtype.slice(prefix.length).trim();
      }
      metadata.case_subtype = matterSubtype;
    }

    // Extract industry
    const industry = $(
      "#GridPlaceHolder_MatterControl1_lblIndustryAffectedValue",
    )
      .text()
      .trim();
    metadata.industry = industry || "Unknown";

    // Extract case number
    const caseNumber = $("#GridPlaceHolder_MatterControl1_lblMatterNumberValue")
      .text()
      .trim();
    if (caseNumber) metadata.case_govid = caseNumber;

    return metadata;
  }

  async getCaseMeta(gov_id: string): Promise<Partial<RawGenericDocket>> {
    // get main page
    const url = `https://documents.dps.ny.gov/public/Common/SearchResults.aspx?MC=1&IA=&MT=&MST=&CN=&MNO=${gov_id}&CO=0&C=&M=&CO=0`;
    console.error(`getting cases metadata for date ${gov_id}`);
    const cases: Partial<RawGenericDocket>[] = await this.getCasesAt(url);
    const basicMetadata = cases[0];

    // If we got basic metadata but industry is Unknown, try to get it from the case page
    if (
      basicMetadata &&
      (basicMetadata.industry === "Unknown" || !basicMetadata.industry)
    ) {
      try {
        const caseUrl = `https://documents.dps.ny.gov/public/MatterManagement/CaseMaster.aspx?MatterCaseNo=${gov_id}`;
        const casePageHtml = await this.getPage(caseUrl, "case-metadata");
        const html = casePageHtml.html();
        const htmlHash = this.calculateBlake2Hash(html);

        // Check cache first
        const cachedIndustry = await this.findCachedParsedData(
          caseUrl,
          "case-metadata",
          htmlHash,
        );
        if (cachedIndustry) {
          basicMetadata.industry = cachedIndustry;
        } else {
          const industry =
            await this.scrapeIndustryAffectedFromFillingsHtml(html);
          basicMetadata.industry = industry;

          // Save industry to cache
          await this.saveParsedDataToCache(
            caseUrl,
            "case-metadata",
            htmlHash,
            industry,
          );
        }
      } catch (error) {
        console.error(
          `Failed to extract industry from case page for ${gov_id}:`,
          error,
        );
      }
    }

    return basicMetadata;
  }

  private async scrapePartiesFromHtml(
    html: string,
  ): Promise<RawGenericParty[]> {
    const $ = cheerio.load(html);
    const parties: RawGenericParty[] = [];
    const partiesTablebodySelector = "#tblActiveParty > tbody";
    const rows = $(`${partiesTablebodySelector} tr`);
    console.error(`Found ${rows.length} party rows.`);

    rows.each((i, row) => {
      const cells = $(row).find("td");
      const nameCellHtml = $(cells[1]).html() || "";
      const emailPhoneCell = $(cells[4]).text();
      const addressCell = $(cells[3]).text();
      const companyCell = $(cells[2]).text();

      // Parse HTML content by splitting on <br> tags to properly handle the structure:
      // <td>Yates, William<br>Director of Research<br>Public Utility Law Project of New York, Inc.</td>
      const nameCellParts = nameCellHtml
        .split(/<br\s*\/?>/i)
        .map((part) => part.trim())
        .filter((part) => part.length > 0);

      console.error(`Row ${i} name cell parts:`, nameCellParts);

      let fullName = "";
      let title = "";
      let firstName = "";
      let lastName = "";

      if (nameCellParts.length > 0) {
        const rawName = nameCellParts[0].trim();

        // Use shared name parsing logic
        const parsedName = this.parsePersonName(rawName);
        fullName = parsedName.fullName;
        firstName = parsedName.firstName;
        lastName = parsedName.lastName;

        // Extract title from second part if available
        if (nameCellParts.length > 1) {
          title = nameCellParts[1].trim();
        }
      }

      // Parse email and phone
      const emailPhoneCellText = $(cells[4]).text();
      let email = "";
      let phone = "";

      const phoneMatch = emailPhoneCellText.match(/Ph:\s*(.*)/);
      if (phoneMatch) {
        phone = phoneMatch[1].trim();
        const emailPart = emailPhoneCellText
          .substring(0, phoneMatch.index)
          .trim();
        if (emailPart.includes("@")) {
          email = emailPart;
        }
      } else if (emailPhoneCellText.includes("@")) {
        email = emailPhoneCellText.trim();
      } else {
        phone = emailPhoneCellText.trim();
      }

      const party: RawGenericParty = {
        name: fullName,
        artifical_person_type: RawArtificalPersonType.Human,
        western_human_last_name: lastName,
        western_human_first_name: firstName,
        human_title: title,
        human_associated_company: companyCell,
        contact_email: email,
        contact_phone: phone,
        contact_address: addressCell,
        extra_metadata: {
          name_cell_html: nameCellHtml,
          name_cell_parts: nameCellParts,
          raw_name_input: nameCellParts[0] || "",
          parsed_name_format: fullName.includes(",")
            ? "last_first"
            : "first_last",
        },
      };

      console.error(`Parsed party ${i}:`, {
        raw_input: nameCellParts[0] || "",
        full_name: fullName,
        first_name: firstName,
        last_name: lastName,
        title: title,
      });

      parties.push(party);
    });

    return parties;
  }

  async fetchFilingMetadata(filingUrl: string): Promise<{
    description?: string;
    filedBy?: string;
    dateFiled?: string;
    filingNo?: string;
    filingOnBehalfOf?: string;
  } | null> {
    try {
      console.error(`Fetching filing metadata from: ${filingUrl}`);
      const $ = await this.getPage(filingUrl, "filing-detail");

      // Calculate hash for cache check
      const html = $.html();
      const htmlHash = this.calculateBlake2Hash(html);

      // Check cache first
      const cachedData = await this.findCachedParsedData(
        filingUrl,
        "filing-detail",
        htmlHash,
      );
      if (cachedData) {
        return cachedData;
      }

      const filingInfo = $("#filing_info");
      if (filingInfo.length === 0) {
        console.error("No filing_info section found on page");
        return null;
      }

      const metadata: any = {};

      // Extract Description of Filing
      const descriptionElement = filingInfo.find("#lblDescriptionofFilingval");
      if (descriptionElement.length > 0) {
        metadata.description = descriptionElement.text().trim();
      }

      // Extract Filed By
      const filedByElement = filingInfo.find("#lblFiledByval");
      if (filedByElement.length > 0) {
        metadata.filedBy = filedByElement.text().trim();
      }

      // Extract Date Filed
      const dateFiledElement = filingInfo.find("#lblDateFiledval");
      if (dateFiledElement.length > 0) {
        metadata.dateFiled = dateFiledElement.text().trim();
      }

      // Extract Filing No.
      const filingNoElement = filingInfo.find("#lblItemNoval");
      if (filingNoElement.length > 0) {
        metadata.filingNo = filingNoElement.text().trim();
      }

      // Extract Filing on behalf of
      const filingOnBehalfOfElement = filingInfo.find(
        "#lblFilingonbehalfofval",
      );
      if (filingOnBehalfOfElement.length > 0) {
        metadata.filingOnBehalfOf = filingOnBehalfOfElement.text().trim();
      }

      console.error(`Extracted filing metadata:`, metadata);

      // Save parsed data to cache
      await this.saveParsedDataToCache(
        filingUrl,
        "filing-detail",
        htmlHash,
        metadata,
      );

      return metadata;
    } catch (error) {
      console.error(`Error fetching filing metadata from ${filingUrl}:`, error);
      return null;
    }
  }

  private async enhanceFilingWithMetadata(
    filing: RawGenericFiling,
  ): Promise<void> {
    try {
      const metadata = await this.fetchFilingMetadata(filing.filing_url);
      if (metadata) {
        // Map description to the filing title/name
        if (metadata.description) {
          filing.name = metadata.description;
          filing.description = metadata.description;
        }

        // Map filedBy to individual_authors_blob
        if (metadata.filedBy) {
          // Format name from "Last,First" to "First Last" format
          const filedByFormatted = this.formatIndividualName(metadata.filedBy);
          if (filedByFormatted) {
            filing.individual_authors_blob = filedByFormatted;
            filing.individual_authors = [filedByFormatted];
          }
        }

        // Map dateFiled to filed_date
        if (metadata.dateFiled) {
          try {
            filing.filed_date = new Date(metadata.dateFiled).toISOString();
          } catch (dateError) {
            console.warn(
              `Invalid date format for filing ${filing.filing_govid}: ${metadata.dateFiled}`,
            );
          }
        }

        // Keep organization filing on behalf of data
        if (metadata.filingOnBehalfOf) {
          filing.organization_authors_blob = metadata.filingOnBehalfOf;
        }

        // Store original metadata in extra_metadata for cross-checking
        filing.extra_metadata = {
          ...filing.extra_metadata,
          fetched_metadata: metadata,
        };
      }
    } catch (error) {
      console.error(
        `Failed to fetch metadata for filing ${filing.filing_govid}:`,
        error,
      );
    }
  }

  private async enhanceFilingsWithMetadataConcurrent(
    filings: RawGenericFiling[],
  ): Promise<void> {
    console.error("Enhancing filings with additional metadata concurrently...");

    // Use the existing processTasksWithQueue method for concurrent processing
    await this.processTasksWithQueue(
      filings,
      async (filing: RawGenericFiling) => {
        await this.enhanceFilingWithMetadata(filing);
        return filing;
      },
      this.max_concurrent_browsers,
    );

    console.error("Finished enhancing all filings with metadata.");
  }

  private extractFilingUrlFromOnclick(onclickAttr: string): string {
    if (!onclickAttr) return "";

    // Parse the onclick: "javascript:return OpenGridPopupWindow('../MatterManagement/MatterFilingItem.aspx','FilingSeq=360722&amp;MatterSeq=64595');"
    const match = onclickAttr.match(
      /OpenGridPopupWindow\('([^']+)','([^']+)'\)/,
    );
    if (!match) return "";

    const [, relativePath, params] = match;
    // Convert ../MatterManagement/MatterFilingItem.aspx to full URL
    const fullPath = relativePath.replace(
      "../",
      "https://documents.dps.ny.gov/public/",
    );
    // Decode HTML entities in parameters (&amp; -> &)
    const decodedParams = params.replace(/&amp;/g, "&");
    return `${fullPath}?${decodedParams}`;
  }

  private parsePersonName(rawName: string): {
    fullName: string;
    firstName: string;
    lastName: string;
  } {
    if (!rawName || !rawName.trim()) {
      return { fullName: "", firstName: "", lastName: "" };
    }

    const trimmedName = rawName.trim();
    let fullName = "";
    let firstName = "";
    let lastName = "";

    // Check if name is in "Last, First" format
    if (trimmedName.includes(",")) {
      const nameComponents = trimmedName.split(",").map((part) => part.trim());
      lastName = nameComponents[0] || "";
      firstName = nameComponents[1] || "";
      fullName = `${firstName} ${lastName}`.trim();
    } else {
      // Assume it's already in "First Last" format or single name
      fullName = trimmedName;
      const nameWords = trimmedName.split(" ");
      if (nameWords.length >= 2) {
        firstName = nameWords.slice(0, -1).join(" ");
        lastName = nameWords[nameWords.length - 1];
      } else {
        firstName = trimmedName;
        lastName = "";
      }
    }

    return { fullName, firstName, lastName };
  }

  private formatIndividualName(name: string): string {
    return this.parsePersonName(name).fullName;
  }

  private async extractFiledDocumentsCount(page: Page): Promise<number> {
    try {
      // Wait for the element to be visible before extracting text
      // Large dockets can take up to 2 minutes to load the count
      await page.waitForSelector("#GridPlaceHolder_lbtPubDoc", {
        state: "visible",
        timeout: 120000,
      });

      // The count is available in the page header before the table loads
      // It's in an element with text like "Filed Documents (1234)"
      const countText = await page.textContent("#GridPlaceHolder_lbtPubDoc");
      console.error(
        `Raw element text for filed documents count: "${countText}"`,
      );

      if (countText) {
        const match = countText.match(/Filed Documents \((\d+)\)/);
        if (match) {
          const count = parseInt(match[1], 10);
          console.error(`Extracted filed documents count: ${count}`);
          return count;
        } else {
          console.warn(
            `Text found but didn't match expected pattern: "${countText}"`,
          );
        }
      } else {
        console.warn("Element found but no text content");
      }
      return 0;
    } catch (error) {
      console.error("Failed to extract filed documents count:", error);
      // Try to capture what's on the page for debugging
      try {
        const allText = await page.evaluate(() => {
          const elem = document.querySelector("#GridPlaceHolder_lbtPubDoc");
          return elem
            ? `Element found: ${elem.textContent}`
            : "Element not found";
        });
        console.error("Debug info:", allText);
      } catch (debugError) {
        console.error("Could not get debug info");
      }
      return 0;
    }
  }

  private async scrapeDocumentsFromHtml(
    html: string,
    tableSelector: string,
    url: string,
    caseGovId: string,
  ): Promise<RawGenericFiling[]> {
    console.error("Starting document extraction from HTML...");
    const $ = cheerio.load(html);
    const filingsMap = new Map<string, RawGenericFiling>();
    const docRows = $(`${tableSelector} tr`);

    console.error(`Found ${docRows.length} document rows.`);

    // First pass: extract basic filing data
    docRows.each((i, docRow) => {
      const docCells = $(docRow).find("td");
      if (docCells.length > 6) {
        const filingNo = $(docCells[5]).text().trim();
        if (!filingNo) return;

        const onclickAttr = $(docCells[5]).find("a").attr("onclick") || "";
        const filingUrl = this.extractFilingUrlFromOnclick(onclickAttr);

        // Skip this filing if we couldn't extract a valid URL
        if (!filingUrl) {
          console.warn(
            `Skipping filing ${filingNo}: could not extract valid URL from onclick attribute`,
          );
          return;
        }

        const documentTitle = $(docCells[3]).find("a").text().trim();
        const attachmentUrlRaw = $(docCells[3]).find("a").attr("href");

        const attachmentUrl = new URL(
          attachmentUrlRaw.replace(
            "../",
            "https://documents.dps.ny.gov/public/",
          ),
          url,
        ).toString();
        const fileName = $(docCells[6]).text().trim();

        if (!filingsMap.has(filingNo)) {
          const documentType = $(docCells[2]).text().trim();
          const dateFiled = $(docCells[1]).text().trim();
          const authors = $(docCells[4]).text().trim();

          filingsMap.set(filingNo, {
            name: documentTitle,
            filed_date: new Date(dateFiled).toISOString(),
            filing_url: filingUrl,
            organization_authors: [],
            individual_authors: [],
            organization_authors_blob: authors,
            individual_authors_blob: "",
            filing_type: documentType,
            description: "",
            attachments: [],
            extra_metadata: { fileName },
            filing_govid: filingNo,
            filing_number: filingNo,
          });
        }

        const filing = filingsMap.get(filingNo);
        if (filing && attachmentUrl) {
          filing.attachments.push({
            name: documentTitle,
            document_extension: fileName.split(".").pop() || "",
            url: attachmentUrl,
            attachment_type: "primary",
            attachment_subtype: "",
            extra_metadata: { fileName },
            attachment_govid: "",
          });
        }
      }
    });

    console.error("Finished basic document extraction.");

    // Second pass: fetch metadata concurrently for all filings
    const filings = Array.from(filingsMap.values());
    if (filings.length > 0) {
      console.error(
        `Fetching metadata concurrently for ${filings.length} filings...`,
      );
      await this.enhanceFilingsWithMetadataConcurrent(filings);
    }

    return filings;
  }

  private async scrapeDocumentsFromSearchPage(
    govId: string,
    caseMetadata: Partial<RawGenericDocket>,
  ): Promise<RawGenericFiling[]> {
    console.error(
      `Scraping documents for ${govId} using monthly search (large docket >1000 documents)`,
    );

    // Get the case start date from metadata
    let startDate: Date;
    if (caseMetadata.opened_date) {
      startDate = new Date(caseMetadata.opened_date);
    } else {
      // If no start date, default to 10 years ago
      startDate = new Date();
      startDate.setFullYear(startDate.getFullYear() - 10);
    }

    const endDate = new Date(); // Current date
    const allFilings: RawGenericFiling[] = [];

    // Iterate month by month
    let currentStart = new Date(startDate);

    while (currentStart <= endDate) {
      // Calculate end of current month
      const currentEnd = new Date(currentStart);
      currentEnd.setMonth(currentEnd.getMonth() + 1);
      currentEnd.setDate(0); // Last day of the month

      // Don't go beyond the overall end date
      if (currentEnd > endDate) {
        currentEnd.setTime(endDate.getTime());
      }

      const startDateStr = this.formatDate(currentStart);
      const endDateStr = this.formatDate(currentEnd);

      console.error(
        `Fetching filings for ${govId} from ${startDateStr} to ${endDateStr}`,
      );

      const url = `https://documents.dps.ny.gov/public/Common/SearchResults.aspx?MC=0&IA=&MT=&MST=&CN=&MNO=${govId}&CO=0&C=&M=&CO=0&DFF=${startDateStr}&DFT=${endDateStr}&DT=&CI=0&FC=`;

      try {
        const $ = await this.getPage(
          url,
          `filing-search-${startDateStr}-to-${endDateStr}`,
        );
        const html = $.html();
        const htmlHash = this.calculateBlake2Hash(html);

        // Check cache first
        const cachedData = await this.findCachedParsedData(
          url,
          "filing-search",
          htmlHash,
        );
        let monthFilings: RawGenericFiling[];

        if (cachedData) {
          monthFilings = cachedData;
        } else {
          // Scrape from the search results table
          const tableSelector = "#tblSearchedDocumentExternal > tbody";
          monthFilings = await this.scrapeDocumentsFromHtml(
            html,
            tableSelector,
            url,
            govId,
          );

          // Save to cache
          await this.saveParsedDataToCache(
            url,
            "filing-search",
            htmlHash,
            monthFilings,
          );
        }

        console.error(
          `Found ${monthFilings.length} filings for month ${startDateStr}`,
        );
        allFilings.push(...monthFilings);
      } catch (error) {
        console.error(
          `Error fetching filings for ${govId} from ${startDateStr} to ${endDateStr}:`,
          error,
        );
      }

      // Move to next month
      currentStart.setMonth(currentStart.getMonth() + 1);
      currentStart.setDate(1); // First day of next month
    }

    console.error(`Total filings scraped for ${govId}: ${allFilings.length}`);
    return allFilings;
  }

  async scrapeDocumentsOnly(
    govId: string,
    keepWindowOpen: boolean = false,
    extractMetadata: boolean = false,
  ): Promise<
    | RawGenericFiling[]
    | {
        documents: RawGenericFiling[];
        page: Page;
        context: any;
        metadata?: Partial<RawGenericDocket>;
      }
  > {
    let windowContext = null;
    try {
      console.error(`Scraping documents for case: ${govId} (new window)`);
      const caseUrl = `https://documents.dps.ny.gov/public/MatterManagement/CaseMaster.aspx?MatterCaseNo=${govId}`;
      const { context, page } = await this.newWindow();
      windowContext = context;

      await page.goto(caseUrl, {
        waitUntil: "domcontentloaded",
        timeout: 180000,
      });
      await page.waitForLoadState("networkidle", { timeout: 150000 });

      const docsTableSelector = "#tblPubDoc > tbody";
      try {
        await page.waitForSelector(docsTableSelector, { timeout: 30_000 });
      } catch {} // Ignore timeout, just means no documents table

      const documentsHtml = await page.content();

      // Calculate hash for cache check
      const htmlHash = this.calculateBlake2Hash(documentsHtml);

      // Extract metadata from the case page if requested
      let metadata: Partial<RawGenericDocket> | undefined;
      if (extractMetadata) {
        try {
          metadata = await this.scrapeMetadataFromCasePageHtml(
            documentsHtml,
            govId,
          );
        } catch (error) {
          console.error(`Error extracting metadata for ${govId}:`, error);
          metadata = { case_govid: govId };
        }
      }

      // Check cache first
      const cachedData = await this.findCachedParsedData(
        caseUrl,
        "case-filings",
        htmlHash,
      );
      if (cachedData) {
        if (keepWindowOpen) {
          return {
            documents: cachedData,
            page,
            context: windowContext,
            metadata,
          };
        }
        await windowContext.close();
        return cachedData;
      }

      // Save HTML snapshot
      await this.saveHtmlSnapshot(documentsHtml, caseUrl, "case-filings");

      const documents = await this.scrapeDocumentsFromHtml(
        documentsHtml,
        docsTableSelector,
        page.url(),
        govId,
      );

      // Save parsed data to cache
      await this.saveParsedDataToCache(
        caseUrl,
        "case-filings",
        htmlHash,
        documents,
      );

      if (keepWindowOpen) {
        return { documents, page, context: windowContext, metadata };
      }

      await windowContext.close();
      return documents;
    } catch (error) {
      console.error(`Error scraping documents for ${govId}:`, error);
      if (windowContext) {
        await windowContext.close();
      }
      if (keepWindowOpen) {
        return { documents: [], page: null as any, context: null };
      }
      return [];
    }
  }

  async scrapePartiesOnly(
    govId: string,
    existingPage?: { page: Page; context: any },
  ): Promise<RawGenericParty[]> {
    let windowContext = null;
    let page: Page;
    const shouldCloseWindow = !existingPage;

    try {
      const caseUrl = `https://documents.dps.ny.gov/public/MatterManagement/CaseMaster.aspx?MatterCaseNo=${govId}`;

      if (existingPage) {
        // Reuse existing page - already on the case page
        page = existingPage.page;
        windowContext = existingPage.context;
      } else {
        // Create new window and navigate
        console.error(`Scraping parties for case: ${govId} (new window)`);
        const newWindow = await this.newWindow();
        windowContext = newWindow.context;
        page = newWindow.page;

        await page.goto(caseUrl);
        await page.waitForLoadState("networkidle");
        await page.waitForTimeout(10000);
      }

      const partiesButtonSelector = "#GridPlaceHolder_lbtContact";

      // Check if parties tab shows (0) - if so, return early
      const partiesButtonText = await page.textContent(partiesButtonSelector);
      if (partiesButtonText && partiesButtonText.includes("(0)")) {
        console.error(`No parties found for case ${govId} - returning early`);
        if (shouldCloseWindow && windowContext) {
          await windowContext.close();
        }
        return [];
      }

      await page.click(partiesButtonSelector);
      await page.waitForLoadState("networkidle");
      await page.waitForTimeout(1000);

      const partiesTableSelector = 'select[name="tblActiveParty_length"]';
      await page.selectOption(partiesTableSelector, "-1");
      await page.waitForLoadState("networkidle");

      await page.waitForFunction(
        () => {
          const tableBody = document.querySelector("#tblActiveParty > tbody");
          if (!tableBody) return false;
          const tableHtml = tableBody.innerHTML;
          return !tableHtml.includes("No data available in table");
        },
        { timeout: 10000 },
      );

      const partiesHtml = await page.content();

      // Calculate hash for cache check
      const htmlHash = this.calculateBlake2Hash(partiesHtml);

      // Check cache first
      const cachedData = await this.findCachedParsedData(
        caseUrl,
        "case-parties",
        htmlHash,
      );
      if (cachedData) {
        if (shouldCloseWindow && windowContext) {
          await windowContext.close();
        }
        return cachedData;
      }

      // Save HTML snapshot
      await this.saveHtmlSnapshot(partiesHtml, caseUrl, "case-parties");

      const parties = await this.scrapePartiesFromHtml(partiesHtml);

      // Save parsed data to cache
      await this.saveParsedDataToCache(
        caseUrl,
        "case-parties",
        htmlHash,
        parties,
      );

      if (shouldCloseWindow && windowContext) {
        await windowContext.close();
      }
      return parties;
    } catch (error) {
      console.error(`Error scraping parties for ${govId}:`, error);
      if (shouldCloseWindow && windowContext) {
        await windowContext.close();
      }
      return [];
    }
  }

  async scrapeMetadataOnly(
    govId: string,
  ): Promise<Partial<RawGenericDocket> | null> {
    try {
      console.error(`Scraping metadata for case: ${govId}`);
      const metadata = await this.getCaseMeta(govId);
      return metadata;
    } catch (error) {
      console.error(`Error scraping metadata for ${govId}:`, error);
      return null;
    }
  }

  async scrapeByPartialDocket(
    partialDockets: Partial<RawGenericDocket>[],
    mode: ScrapingMode,
  ): Promise<Partial<RawGenericDocket>[]> {
    // Collect valid case_govid values
    const govIds = partialDockets
      .map((d) => d.case_govid)
      .filter(
        (id): id is string => typeof id === "string" && id.trim().length > 0,
      );

    // Call the existing scraper with those IDs
    return this.scrapeByGovIds(govIds, mode);
  }

  async scrapeByGovIds(
    govIds: string[],
    mode: ScrapingMode,
  ): Promise<Partial<RawGenericDocket>[]> {
    console.error(
      `Scraping ${govIds.length} cases in ${mode} mode (parallel processing)`,
    );

    const processId = async (
      govId: string,
    ): Promise<Partial<RawGenericDocket> | null> => {
      try {
        switch (mode) {
          case ScrapingMode.METADATA:
            return await this.scrapeMetadataOnly(govId);

          case ScrapingMode.filing: {
            const result = await this.scrapeDocumentsOnly(govId);
            const filings = Array.isArray(result) ? result : result.documents;
            return { case_govid: govId, filings };
          }

          case ScrapingMode.PARTIES: {
            const parties = await this.scrapePartiesOnly(govId);
            return { case_govid: govId, case_parties: parties };
          }

          case ScrapingMode.ALL: {
            // Scrape documents with keepWindowOpen=true and extractMetadata=true
            // This opens ONE window and extracts both documents and metadata from the case page
            const docsResult = await this.scrapeDocumentsOnly(
              govId,
              true,
              true,
            );
            let documents: RawGenericFiling[];
            let parties: RawGenericParty[];
            let metadata: Partial<RawGenericDocket> | undefined;

            if (typeof docsResult === "object" && "documents" in docsResult) {
              documents = docsResult.documents;
              metadata = docsResult.metadata;

              // Reuse the same window for parties if page exists
              if (docsResult.page && docsResult.context) {
                try {
                  parties = await this.scrapePartiesOnly(govId, {
                    page: docsResult.page,
                    context: docsResult.context,
                  });
                } finally {
                  // Close the window after parties scraping
                  await docsResult.context.close();
                }
              } else {
                // Documents scraping failed, open new window for parties
                parties = await this.scrapePartiesOnly(govId);
              }
            } else {
              // Fallback: shouldn't happen but handle it
              documents = docsResult as RawGenericFiling[];
              parties = await this.scrapePartiesOnly(govId);
            }

            let return_case: Partial<RawGenericDocket> = metadata || {
              case_govid: govId,
            };
            return_case.case_govid = govId;
            return_case.filings = documents;
            return_case.case_parties = parties;

            return return_case;
          }
        }
      } catch (error) {
        console.error(`Error processing ${govId} in mode ${mode}:`, error);
        return null;
      }
    };
    const processIdAndUpload = async (
      govID: string,
    ): Promise<Partial<RawGenericDocket> | null> => {
      let return_result = null;
      try {
        return_result = await processId(govID);
        if (return_result !== null) {
          console.error(return_result.case_govid);
          // Upload disabled
          // try {
          //   await pushResultsToUploader([return_result], mode);
          // } catch (e) {
          //   console.error(e);
          // }
        } else {
          console.error("Result was equal to null.");
        }
        return return_result;
      } catch (err) {
        console.error(err);
        return return_result;
      }
    };

    const results = await this.processTasksWithQueue(
      govIds,
      processIdAndUpload,
    );
    return results.filter(
      (result): result is Partial<RawGenericDocket> => result !== null,
    );
  }

  formatDate(date: Date): string {
    const month = (date.getMonth() + 1).toString().padStart(2, "0");
    const day = date.getDate().toString().padStart(2, "0");
    const year = date.getFullYear();
    return `${month}/${day}/${year}`;
  }

  createSearchUrl(beginDate: Date, endDate: Date): string {
    const formattedBeginDate = this.formatDate(beginDate);
    const formattedEndDate = this.formatDate(endDate);
    return `https://documents.dps.ny.gov/public/Common/SearchResults.aspx?MC=0&IA=&MT=&MST=&CN=&C=&M=&CO=0&DFF=${formattedBeginDate}&DFT=${formattedEndDate}&DT=&CI=0&FC=`;
  }

  async getDocketIdsWithFilingsBetweenDates(
    beginDate: Date,
    endDate: Date,
  ): Promise<string[]> {
    const url = this.createSearchUrl(beginDate, endDate);
    const $ = await this.getPage(url);
    const docketGovIds: string[] = [];

    $("#tblSearchedDocumentExternal > tbody:nth-child(3) tr").each((i, row) => {
      const cells = $(row).find("td");
      const docketGovId = $(cells[4]).find("a").text().trim();
      if (docketGovId) {
        docketGovIds.push(docketGovId);
      }
    });
    const govid_set = new Set(docketGovIds);

    return [...govid_set]; // Return unique values
  }

  async getCasesBetweenDates(
    beginDate: Date,
    endDate: Date,
  ): Promise<Partial<RawGenericDocket>[]> {
    let docket_ids = await this.getDocketIdsWithFilingsBetweenDates(
      beginDate,
      endDate,
    );
    let dockets = docket_ids.map((id) => {
      return { case_govid: id };
    });
    return dockets;
  }

  async getTodaysFilingsWorkflow(
    mode: ScrapingMode = ScrapingMode.ALL,
  ): Promise<Partial<RawGenericDocket>[]> {
    console.error("Starting today's filings workflow...");

    // Get today's date
    const today = new Date();
    const formattedDate = this.formatDate(today);
    console.error(`Searching for filings from: ${formattedDate}`);

    // Get all unique case numbers that had filings today
    const caseNumbers = await this.getDocketIdsWithFilingsBetweenDates(
      today,
      today,
    );

    console.error(
      `Found ${caseNumbers.length} cases with filings today: ${caseNumbers.join(", ")}`,
    );

    if (caseNumbers.length === 0) {
      console.error("No filings found for today");
      return [];
    }

    // Run full scraping pipeline for each case
    console.error(
      `Scraping complete data for ${caseNumbers.length} cases in ${mode} mode`,
    );
    const results = await this.scrapeByGovIds(caseNumbers, mode);

    console.error(
      `Today's filings workflow complete. Processed ${results.length} cases.`,
    );
    return results;
  }
}

function parseArguments(): ScrapingOptions | null {
  const args = process.argv.slice(2);

  if (args.length === 0) {
    return null; // No arguments, will be handled in main
  }

  let mode = ScrapingMode.ALL;
  let govIds: string[] = [];
  let dateString: string | undefined;
  let beginDate: string | undefined;
  let endDate: string | undefined;
  let fromFile: string | undefined;
  let outFile: string | undefined;
  let headed = false; // Default to headless
  let missing = false; // Default to not checking for missing
  let todayFilings = false; // Default to not running today's filings workflow
  let intermediateDir: string | undefined;

  for (let i = 0; i < args.length; i++) {
    const arg = args[i];

    if (arg === "--mode") {
      const modeValue = args[++i];
      if (Object.values(ScrapingMode).includes(modeValue as ScrapingMode)) {
        mode = modeValue as ScrapingMode;
      } else {
        throw new Error(
          `Invalid mode: ${modeValue}. Valid modes: ${Object.values(
            ScrapingMode,
          ).join(", ")}`,
        );
      }
    } else if (arg === "--gov-ids") {
      const idsString = args[++i];
      govIds = idsString
        .split(",")
        .map((id) => id.trim())
        .filter((id) => id.length > 0);
    } else if (arg === "--date") {
      dateString = args[++i];
    } else if (arg === "--begin-date") {
      beginDate = args[++i];
    } else if (arg === "--end-date") {
      endDate = args[++i];
    } else if (arg === "--from-file") {
      fromFile = args[++i];
    } else if (arg === "-o" || arg === "--outfile") {
      outFile = args[++i];
    } else if (arg === "--headed") {
      headed = true;
    } else if (arg === "--missing") {
      missing = true;
    } else if (arg === "--today-filings") {
      todayFilings = true;
    } else if (arg === "--intermediate-dir") {
      intermediateDir = args[++i];
    } else if (!arg.startsWith("--")) {
      // Backward compatibility for JSON array
      try {
        const parsed = JSON.parse(arg);
        if (Array.isArray(parsed)) {
          return null; // Let old runCli handle this
        }
      } catch {
        // Not JSON, ignore
      }
    }
  }

  // Load gov IDs from file if specified
  if (fromFile) {
    try {
      const fileContent = fs.readFileSync(fromFile, "utf-8");
      const fileIds = fileContent
        .split("\n")
        .map((line) => line.trim())
        .filter((line) => line.length > 0 && !line.startsWith("#"));
      govIds.push(...fileIds);
    } catch (error) {
      throw new Error(`Error reading file ${fromFile}: ${error}`);
    }
  }

  return {
    mode,
    govIds,
    dateString,
    beginDate,
    endDate,
    fromFile,
    outFile,
    headed,
    missing,
    todayFilings,
    intermediateDir,
  };
}

async function saveResultsToFile(
  results: any[],
  outFile: string,
  mode: ScrapingMode,
) {
  try {
    const jsonData = JSON.stringify(results, null, 2);
    fs.writeFileSync(outFile, jsonData, "utf-8");
    console.error(
      `✅ Successfully saved ${results.length} results to ${outFile}`,
    );
    console.error(
      `📊 Mode: ${mode}, File size: ${(jsonData.length / 1024).toFixed(2)} KB`,
    );
  } catch (error) {
    console.error(`❌ Error writing to file ${outFile}:`, error);
    throw error;
  }
}
// TODO: This should 100% be in the }task handling layer.
// (And actually it might be a good idea for the task runner to just save the stuff to s3 directly? Food for thought.)
async function pushResultsToUploader(
  results: Partial<RawGenericDocket>[],
  mode: ScrapingMode,
) {
  let upload_type = "all";
  if (mode == ScrapingMode.filing) {
    upload_type = "only_filing";
  }
  if (mode == ScrapingMode.PARTIES) {
    upload_type = "only_parties";
  }
  if (mode == ScrapingMode.METADATA) {
    upload_type = "only_metadata";
  }
  if (mode == ScrapingMode.ALL) {
    upload_type = "all";
  }
  const url = "http://localhost:33399/admin/cases/upload_raw";

  console.error(`Uploading ${results.length} with mode ${mode} to uploader.`);

  try {
    const payload = results.map((docket) => ({
      docket,
      upload_type,
      jurisdiction: {
        country: "usa",
        jurisdiction: "ny_puc",
        state: "ny",
      },
    }));
    const response = await fetch(url, {
      method: "POST",
      headers: {
        accept: "application/json",
        "Content-Type": "application/json",
      },
      body: JSON.stringify(payload),
    });

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(
        `Upload failed: ${response.status} ${response.statusText} - ${errorText}`,
      );
    }
    let govid_list = results.map((x) => x.case_govid);
    console.error(`Successfully uploaded dockets to s3: ${govid_list}`);

    return "successfully uploaded docket";
  } catch (err) {
    console.error("Error uploading results:", err);
    // throw err;
    return null;
  }
}

async function runCustomScraping(
  scraper: NyPucScraper,
  options: ScrapingOptions,
) {
  console.error(`Running custom scraping with options:`, options);

  // Handle today's filings workflow
  if (options.todayFilings) {
    console.error("Running today's filings workflow...");
    const results = await scraper.getTodaysFilingsWorkflow(options.mode);
    console.error(
      `Today's filings workflow completed with ${results.length} results`,
    );

    // Save or output results
    if (options.outFile) {
      await saveResultsToFile(results, options.outFile, options.mode);
    } else {
      console.log(JSON.stringify(results, null, 2));
    }
    return;
  }

  let casesToScrape: Partial<RawGenericDocket>[] = [];
  let govIds: string[] = [];

  // Determine which cases to scrape based on provided arguments
  if (options.govIds && options.govIds.length > 0) {
    // Explicit gov IDs provided
    govIds = options.govIds;
    console.error(`Using ${govIds.length} explicitly provided gov IDs`);
  } else if (options.dateString) {
    // Single date provided
    console.error(`Getting cases for date: ${options.dateString}`);
    casesToScrape = await scraper.getDateCases(options.dateString);
    console.error(
      `Found ${casesToScrape.length} cases for date ${options.dateString}`,
    );
  } else if (options.beginDate && options.endDate) {
    // Date range provided
    console.error(
      `Getting cases between ${options.beginDate} and ${options.endDate}`,
    );
    casesToScrape = await scraper.getCasesBetweenDates(
      new Date(options.beginDate),
      new Date(options.endDate),
    );
    console.error(
      `Found ${casesToScrape.length} cases between ${options.beginDate} and ${options.endDate}`,
    );
  } else {
    // No specific cases provided
    if (options.missing) {
      console.error("No specific cases provided, getting all missing cases");
      casesToScrape = await scraper.getAllMissingCaseList();
      console.error(`Found ${casesToScrape.length} missing cases`);
    } else {
      console.error("No specific cases provided, getting all cases");
      casesToScrape = await scraper.getAllCaseList();
      console.error(`Found ${casesToScrape.length} cases`);
    }
  }

  // Extract gov IDs from cases if we have partial dockets
  if (casesToScrape.length > 0) {
    govIds = casesToScrape
      .map((c) => c.case_govid)
      .filter(
        (id): id is string => typeof id === "string" && id.trim().length > 0,
      );
  }

  if (govIds.length === 0) {
    throw new Error("No cases found to scrape");
  }

  // Now scrape the cases with the specified mode
  console.error(`Scraping ${govIds.length} cases in ${options.mode} mode`);
  const results = await scraper.scrapeByGovIds(govIds, options.mode);

  console.error(`Scraped ${results.length} results in ${options.mode} mode`);

  // Save or output results
  if (options.outFile) {
    await saveResultsToFile(results, options.outFile, options.mode);
  } else {
    console.log(JSON.stringify(results, null, 2));
  }
}

async function main() {
  let browser: Browser | null = null;
  try {
    const customOptions = parseArguments();

    if (!customOptions) {
      console.error("Error: No scraping arguments provided. Exiting.");
      console.error(
        "Please provide arguments to run the scraper, e.g. --mode full-all-missing",
      );
      process.exit(1);
    }

    if (!customOptions.intermediateDir) {
      console.error(
        "ℹ️  No --intermediate-dir specified. Intermediate files will not be saved.",
      );
    }

    // Launch the browser based on the 'headed' option
    browser = await chromium.launch({ headless: !customOptions.headed });
    const context = await browser.newContext();
    const rootpage = await context.newPage();
    const scraper = new NyPucScraper(
      rootpage,
      context,
      browser,
      customOptions.intermediateDir || "",
    );

    // Use custom scraping logic
    await runCustomScraping(scraper, customOptions);
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
