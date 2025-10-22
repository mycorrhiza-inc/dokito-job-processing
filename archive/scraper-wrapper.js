const { Page, Browser } = require('playwright');
const cheerio = require('cheerio');

// Scraping modes enum
const ScrapingMode = {
  ALL: "all",
  FULL: "full",
  METADATA: "meta",
  DOCUMENTS: "docs",
  PARTIES: "parties",
  CASE_LIST: "case-list",
};

// Data types
const RawArtificalPersonType = {
  Human: "human",
  Organization: "organization"
};

class NyPucScraper {
  constructor(page, context, browser, logger = null) {
    this.state = "ny";
    this.jurisdiction_name = "ny_puc";
    this.context = context;
    this.browser = browser;
    this.rootPage = page;
    this.max_tabs = 10;
    this.max_concurrent_browsers = 10;
    this.pageCache = new Map();
    this.urlTable = {};
    this.pageFetchPromises = {};
    this.maxCacheSize = 100;
    this.logger = logger;
  }

  // Logging helper methods
  log(level, message, metadata = {}) {
    if (this.logger) {
      this.logger[level](message, metadata);
    } else {
      console.log(`[${level.toUpperCase()}] ${message}`, metadata);
    }
  }

  info(message, metadata = {}) {
    this.log('info', message, metadata);
  }

  debug(message, metadata = {}) {
    this.log('debug', message, metadata);
  }

  warn(message, metadata = {}) {
    this.log('warn', message, metadata);
  }

  error(message, metadata = {}) {
    this.log('error', message, metadata);
  }

  clearCache() {
    this.pageCache.clear();
    this.urlTable = {};
    this.debug("Cleared page cache and URL table");
  }

  pages() {
    return this.context.pages();
  }

  async newTab() {
    const page = await this.context.newPage();
    return page;
  }

  async newWindow() {
    const context = await this.browser.newContext();
    const page = await context.newPage();
    return { context, page };
  }

  async processTasksWithQueue(tasks, taskProcessor, maxConcurrent = this.max_concurrent_browsers) {
    return new Promise((resolve, reject) => {
      const results = [];
      const errors = [];
      let currentIndex = 0;
      let completed = 0;
      let running = 0;

      const processNext = async () => {
        if (currentIndex >= tasks.length) return;
        if (running >= maxConcurrent) return;

        const taskIndex = currentIndex++;
        const task = tasks[taskIndex];
        running++;

        this.info(`Starting task ${taskIndex + 1}/${tasks.length}`, { running, max_concurrent: maxConcurrent });

        try {
          const result = await taskProcessor(task);
          results[taskIndex] = result;
        } catch (error) {
          this.error(`Task ${taskIndex + 1} failed`, { error: error.message, stack: error.stack });
          errors.push(error);
          results[taskIndex] = null;
        } finally {
          running--;
          completed++;
          this.debug(`Completed task ${taskIndex + 1}/${tasks.length}`, { running });

          if (completed === tasks.length) {
            if (errors.length > 0) {
              this.warn(`${errors.length} tasks failed, but continuing with successful results`, { error_count: errors.length, total_tasks: tasks.length });
            }
            resolve(results.filter((r) => r !== null));
          } else {
            while (running < maxConcurrent && currentIndex < tasks.length) {
              processNext();
            }
          }
        }
      };

      const initialBatch = Math.min(maxConcurrent, tasks.length);
      for (let i = 0; i < initialBatch; i++) {
        processNext();
      }
    });
  }

  async getPage(url) {
    this.urlTable[url] = url;
    const cached = this.pageCache.get(url);
    if (cached !== undefined) {
      return cached;
    }

    if (this.pageFetchPromises[url]) {
      return await this.pageFetchPromises[url];
    }

    this.pageFetchPromises[url] = this.fetchPageContent(url);
    const result = await this.pageFetchPromises[url];

    delete this.pageFetchPromises[url];
    return result;
  }

  async fetchPageContent(url) {
    const { context, page } = await this.newWindow();
    try {
      await page.goto(url);
      await page.waitForLoadState("networkidle");
      this.debug(`Fetching page content`, { url });
      const html = await page.content();

      const $ = cheerio.load(html);

      if (this.pageCache.size >= this.maxCacheSize) {
        const firstKey = this.pageCache.keys().next().value;
        this.pageCache.delete(firstKey);
      }
      this.pageCache.set(url, $);
      return $;
    } finally {
      await context.close();
    }
  }

  async getCaseTitle(matter_seq) {
    const url = `https://documents.dps.ny.gov/public/MatterManagement/ExpandTitle.aspx?MatterSeq=${matter_seq}`;
    const $ = await this.getPage(url);
    return $("$txtTitle");
  }

  async getCasePage(gov_id) {
    const url = `https://documents.dps.ny.gov/public/MatterManagement/CaseMaster.aspx?MatterCaseNo=${gov_id}`;
    const page = await this.getPage(url);
    return page;
  }

  async getDateCases(dateString) {
    const url = `https://documents.dps.ny.gov/public/Common/SearchResults.aspx?MC=1&IA=&MT=&MST=&CN=&SDT=${dateString}&SDF=${dateString}&C=&M=&CO=0`;
    this.info(`Getting cases for date`, { date: dateString });
    const cases = await this.getCasesAt(url);
    return cases;
  }

  async getRows($) {
    return $("#tblSearchedMatterExternal > tbody tr");
  }

  async getCasesAt(url) {
    const cases = [];
    this.debug(`Navigating to search results`, { url });
    const $ = await this.getPage(url);

    this.debug("Extracting industry affected data");
    let industry_affected = $("#GridPlaceHolder_lblSearchCriteriaValue").text().trim();

    if (industry_affected.startsWith("Industry Affected:")) {
      industry_affected = industry_affected.replace("Industry Affected:", "").trim();
    } else {
      industry_affected = "Unknown";
    }

    this.debug("Extracting case rows from table");
    const rows = await this.getRows($);
    this.info(`Found case rows`, { count: rows.length });

    rows.each((i, row) => {
      const cells = $(row).find("td");
      if (cells.length >= 6) {
        this.debug("Extracting case data from row");
        const case_govid = $(cells[0]).find("a").text();
        const case_url = `https://documents.dps.ny.gov/public/MatterManagement/CaseMaster.aspx?MatterCaseNo=${case_govid}`;
        const matter_type = $(cells[1]).text();
        const matter_subtype = $(cells[2]).text();
        const opened_date = $(cells[3]).text();
        const case_name = $(cells[4]).text();
        const petitioner = $(cells[5]).text();

        const caseData = {
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
        this.debug(`Successfully extracted case`, { gov_id: case_govid });
      }
    });
    return cases;
  }

  async getCaseList() {
    const industryNumbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];

    const scrapeIndustry = async (industry_number) => {
      this.info(`Processing industry`, { industry_number });
      const industry_url = `https://documents.dps.ny.gov/public/Common/SearchResults.aspx?MC=1&IA=${industry_number}`;
      return await this.getCasesAt(industry_url);
    };

    this.info(`Scraping industries`, { industry_count: industryNumbers.length, max_concurrent: 10 });
    const industryResults = await this.processTasksWithQueue(
      industryNumbers,
      scrapeIndustry,
      10
    );

    return industryResults.flat();
  }

  async getCaseMeta(gov_id) {
    const url = `https://documents.dps.ny.gov/public/Common/SearchResults.aspx?MC=1&IA=&MT=&MST=&CN=&MNO=${gov_id}&CO=0&C=&M=&CO=0`;
    this.debug(`Getting case metadata`, { gov_id });
    const cases = await this.getCasesAt(url);
    return cases[0];
  }

  async scrapeDocumentsFromHtml(html, tableSelector, url) {
    this.debug("Starting document extraction from HTML");
    const $ = cheerio.load(html);
    const filingsMap = new Map();
    const docRows = $(`${tableSelector} tr`);

    this.info(`Found document rows`, { count: docRows.length });

    const rowsArray = docRows.toArray();
    const chunkSize = 50;
    const chunks = [];

    for (let i = 0; i < rowsArray.length; i += chunkSize) {
      chunks.push(rowsArray.slice(i, i + chunkSize));
    }

    for (const chunk of chunks) {
      chunk.forEach(docRow => {
        const docCells = $(docRow).find("td");
        if (docCells.length > 6) {
          const filingNo = $(docCells[5]).text().trim();
          if (!filingNo) return;

          const documentTitle = $(docCells[3]).find("a").text().trim();
          const attachmentUrl = $(docCells[3]).find("a").attr("href");
          const fileName = $(docCells[6]).text().trim();

          if (!filingsMap.has(filingNo)) {
            const documentType = $(docCells[2]).text().trim();
            const dateFiled = $(docCells[1]).text().trim();
            const authors = $(docCells[4]).text().trim();

            filingsMap.set(filingNo, {
              name: documentTitle,
              filed_date: new Date(dateFiled).toISOString(),
              organization_authors: [],
              individual_authors: [],
              organization_authors_blob: authors,
              individual_authors_blob: "",
              filing_type: documentType,
              description: "",
              attachments: [],
              extra_metadata: { fileName },
              filling_govid: filingNo,
            });
          }

          const filing = filingsMap.get(filingNo);
          if (filing && attachmentUrl) {
            filing.attachments.push({
              name: documentTitle,
              document_extension: fileName.split(".").pop() || "",
              url: new URL(
                attachmentUrl.replace("../", "https://documents.dps.ny.gov/public/"),
                url
              ).toString(),
              attachment_type: "primary",
              attachment_subtype: "",
              extra_metadata: { fileName },
              attachment_govid: "",
            });
          }
        }
      });

      await new Promise(resolve => setImmediate(resolve));
    }

    this.debug("Finished document extraction");
    return Array.from(filingsMap.values());
  }

  async scrapePartiesFromHtml(html) {
    const $ = cheerio.load(html);
    const parties = [];
    const rows = $("#grdParty tr.gridrow, #grdParty tr.gridaltrow");
    this.info(`Found party rows`, { count: rows.length });

    rows.each((i, row) => {
      const cells = $(row).find("td");
      const nameCell = $(cells[1]).text();
      const emailPhoneCell = $(cells[4]).text();
      const addressCell = $(cells[3]).text();
      const companyCell = $(cells[2]).text();

      const nameParts = nameCell.split("\n");
      const fullName = nameParts[0];
      const title = nameParts.length > 1 ? nameParts[1] : "";

      const emailPhoneParts = emailPhoneCell.split("\n");
      const email = emailPhoneParts.find((part) => part.includes("@")) || "";
      const phone = emailPhoneParts.find((part) => part.startsWith("Ph:")) || "";

      const party = {
        name: fullName,
        artifical_person_type: RawArtificalPersonType.Human,
        western_human_last_name: fullName.split(" ")[0] || "",
        western_human_first_name: fullName.split(" ").slice(1).join(" ") || "",
        human_title: title,
        human_associated_company: companyCell,
        contact_email: email,
        contact_phone: phone,
        contact_address: addressCell,
      };
      parties.push(party);
    });

    return parties;
  }

  async scrapeDocumentsOnly(govIds) {
    this.info(`Scraping documents`, { case_count: govIds.length, max_concurrent: this.max_concurrent_browsers });

    const scrapeDocumentsForId = async (govId) => {
      let windowContext = null;
      try {
        this.debug(`Scraping documents for case`, { gov_id: govId });
        const caseUrl = `https://documents.dps.ny.gov/public/MatterManagement/CaseMaster.aspx?MatterCaseNo=${govId}`;
        const { context, page } = await this.newWindow();
        windowContext = context;

        await page.goto(caseUrl);
        await page.waitForLoadState("networkidle");

        const docsTableSelector = "#tblPubDoc > tbody";
        try {
          await page.waitForSelector(docsTableSelector, { timeout: 30000 });
        } catch { }

        const documentsHtml = await page.content();
        const documents = await this.scrapeDocumentsFromHtml(
          documentsHtml,
          docsTableSelector,
          page.url()
        );

        documents.forEach(doc => {
          if (!doc.extra_metadata) doc.extra_metadata = {};
          doc.extra_metadata.case_govid = govId;
        });

        await windowContext.close();
        windowContext = null;
        return documents;
      } catch (error) {
        this.error(`Error scraping documents for case`, { gov_id: govId, error: error.message });
        if (windowContext) {
          await windowContext.close();
        }
        return [];
      }
    };

    const documentsArrays = await this.processTasksWithQueue(govIds, scrapeDocumentsForId);
    return documentsArrays.flat();
  }

  async scrapePartiesOnly(govIds) {
    console.log(`Scraping parties for ${govIds.length} cases with max ${this.max_concurrent_browsers} concurrent browsers`);

    const scrapePartiesForId = async (govId) => {
      let windowContext = null;
      try {
        console.log(`Scraping parties for case: ${govId} (new window)`);
        const caseUrl = `https://documents.dps.ny.gov/public/MatterManagement/CaseMaster.aspx?MatterCaseNo=${govId}`;
        const { context, page } = await this.newWindow();
        windowContext = context;

        await page.goto(caseUrl);
        await page.waitForLoadState("networkidle");

        const partiesButtonSelector = "#GridPlaceHolder_lbtContact";
        await page.click(partiesButtonSelector);
        await page.waitForLoadState("networkidle");

        const partiesTableSelector = 'select[name="tblActiveParty_length"]';
        await page.selectOption(partiesTableSelector, "-1");
        await page.waitForLoadState("networkidle");

        const partiesHtml = await page.content();
        const parties = await this.scrapePartiesFromHtml(partiesHtml);

        parties.forEach(party => {
          if (!party.extra_metadata) party.extra_metadata = {};
          party.extra_metadata.case_govid = govId;
        });

        await windowContext.close();
        windowContext = null;
        return parties;
      } catch (error) {
        console.error(`Error scraping parties for ${govId}:`, error);
        if (windowContext) {
          await windowContext.close();
        }
        return [];
      }
    };

    const partiesArrays = await this.processTasksWithQueue(govIds, scrapePartiesForId);
    return partiesArrays.flat();
  }

  async scrapeMetadataOnly(govIds) {
    console.log(`Scraping metadata for ${govIds.length} cases with max ${this.max_concurrent_browsers} concurrent browsers`);

    const scrapeMetadataForId = async (govId) => {
      try {
        console.log(`Scraping metadata for case: ${govId}`);
        const metadata = await this.getCaseMeta(govId);
        return metadata;
      } catch (error) {
        console.error(`Error scraping metadata for ${govId}:`, error);
        return null;
      }
    };

    const metadataResults = await this.processTasksWithQueue(govIds, scrapeMetadataForId);
    return metadataResults.filter(metadata => metadata !== null);
  }

  async scrapeByGovIds(govIds, mode) {
    console.log(`Scraping ${govIds.length} cases in ${mode} mode (parallel processing)`);

    switch (mode) {
      case 'all':
      case 'full':
        console.log(`Running full scraping for ${govIds.length} cases with max ${this.max_concurrent_browsers} concurrent browsers`);

        if (govIds.length > 10) {
          console.log("Using batch optimization for large dataset");

          const [metadataResults, documentsResults, partiesResults] = await Promise.all([
            this.scrapeMetadataOnly(govIds),
            this.scrapeDocumentsOnly(govIds),
            this.scrapePartiesOnly(govIds)
          ]);

          const fullResults = metadataResults.map(metadata => {
            if (metadata && metadata.case_govid) {
              const caseDocuments = documentsResults.filter(doc =>
                doc.extra_metadata?.case_govid === metadata.case_govid
              );
              const caseParties = partiesResults.filter(party =>
                party.extra_metadata?.case_govid === metadata.case_govid
              );

              metadata.filings = caseDocuments;
              metadata.case_parties = caseParties;
              return metadata;
            }
            return null;
          });

          this.clearCache();
          return fullResults.filter(result => result !== null);
        } else {
          const scrapeFullForId = async (govId) => {
            try {
              const [metadata, documents, parties] = await Promise.all([
                this.getCaseMeta(govId),
                this.scrapeDocumentsOnly([govId]),
                this.scrapePartiesOnly([govId]),
              ]);

              if (metadata) {
                metadata.filings = documents;
                metadata.case_parties = parties;
                return metadata;
              }
              return null;
            } catch (error) {
              console.error(`Error processing ${govId}:`, error);
              return null;
            }
          };

          const fullResults = await this.processTasksWithQueue(govIds, scrapeFullForId);
          this.clearCache();
          return fullResults.filter(result => result !== null);
        }

      case 'meta':
        return await this.scrapeMetadataOnly(govIds);

      case 'docs':
        return await this.scrapeDocumentsOnly(govIds);

      case 'parties':
        return await this.scrapePartiesOnly(govIds);

      case 'full-extraction':
        console.log(`Running enhanced full extraction for ${govIds.length} cases with max ${this.max_concurrent_browsers} concurrent browsers`);

        if (govIds.length > 10) {
          console.log("Using batch optimization for large dataset");

          const [metadataResults, documentsResults, partiesResults] = await Promise.all([
            this.scrapeMetadataOnly(govIds),
            this.scrapeDocumentsOnly(govIds),
            this.scrapePartiesOnly(govIds)
          ]);

          const extractionResults = metadataResults.map(metadata => {
            if (metadata && metadata.case_govid) {
              const caseDocuments = documentsResults.filter(doc =>
                doc.extra_metadata?.case_govid === metadata.case_govid
              );
              const caseParties = partiesResults.filter(party =>
                party.extra_metadata?.case_govid === metadata.case_govid
              );

              metadata.filings = caseDocuments;
              metadata.case_parties = caseParties;
              return metadata;
            }
            return null;
          });

          return extractionResults.filter(result => result !== null);
        } else {
          const scrapeExtractionForId = async (govId) => {
            try {
              const [metadata, documents, parties] = await Promise.all([
                this.getCaseMeta(govId),
                this.scrapeDocumentsOnly([govId]),
                this.scrapePartiesOnly([govId]),
              ]);

              if (metadata) {
                metadata.filings = documents;
                metadata.case_parties = parties;
                return metadata;
              }
              return null;
            } catch (error) {
              console.error(`Error in full extraction for ${govId}:`, error);
              return null;
            }
          };

          const extractionResults = await this.processTasksWithQueue(govIds, scrapeExtractionForId);
          return extractionResults.filter(result => result !== null);
        }

      default:
        throw new Error(`Unsupported scraping mode: ${mode}`);
    }
  }

  formatDate(date) {
    const month = (date.getMonth() + 1).toString().padStart(2, "0");
    const day = date.getDate().toString().padStart(2, "0");
    const year = date.getFullYear();
    return `${month}/${day}/${year}`;
  }

  createSearchUrl(beginDate, endDate) {
    const formattedBeginDate = this.formatDate(beginDate);
    const formattedEndDate = this.formatDate(endDate);
    return `https://documents.dps.ny.gov/public/Common/SearchResults.aspx?MC=0&IA=&MT=&MST=&CN=&C=&M=&CO=0&DFF=${formattedBeginDate}&DFT=${formattedEndDate}&DT=&CI=0&FC=`;
  }

  async getFilingsBetweenDates(beginDate, endDate) {
    const daysDifference = Math.ceil((endDate.getTime() - beginDate.getTime()) / (1000 * 60 * 60 * 24));

    if (daysDifference > 30) {
      console.log(`Large date range detected (${daysDifference} days). Using parallel chunked processing.`);
      return await this.getFilingsBetweenDatesParallel(beginDate, endDate);
    }

    const url = this.createSearchUrl(beginDate, endDate);
    const $ = await this.getPage(url);
    const docketGovIds = [];

    $("#tblSearchedDocumentExternal > tbody:nth-child(3) tr").each((i, row) => {
      const cells = $(row).find("td");
      const docketGovId = $(cells[4]).find("a").text().trim();
      if (docketGovId) {
        docketGovIds.push(docketGovId);
      }
    });

    const govid_set = new Set(docketGovIds);
    return Array.from(govid_set);
  }

  async getFilingsBetweenDatesParallel(beginDate, endDate) {
    const totalDays = Math.ceil((endDate.getTime() - beginDate.getTime()) / (1000 * 60 * 60 * 24));
    const chunkSizeDays = Math.min(7, totalDays);
    const chunks = [];

    let currentDate = new Date(beginDate);
    while (currentDate <= endDate) {
      const chunkEnd = new Date(currentDate);
      chunkEnd.setDate(chunkEnd.getDate() + chunkSizeDays - 1);

      if (chunkEnd > endDate) {
        chunkEnd.setTime(endDate.getTime());
      }

      chunks.push({
        start: new Date(currentDate),
        end: new Date(chunkEnd)
      });

      currentDate.setDate(currentDate.getDate() + chunkSizeDays);

      if (chunkEnd.getTime() === endDate.getTime()) {
        break;
      }
    }

    console.log(`Processing ${chunks.length} date chunks in parallel (${totalDays} total days, ${chunkSizeDays} days per chunk)`);

    const scrapeChunk = async (chunk) => {
      try {
        const url = this.createSearchUrl(chunk.start, chunk.end);
        const $ = await this.getPage(url);
        const docketGovIds = [];

        $("#tblSearchedDocumentExternal > tbody:nth-child(3) tr").each((i, row) => {
          const cells = $(row).find("td");
          const docketGovId = $(cells[4]).find("a").text().trim();
          if (docketGovId) {
            docketGovIds.push(docketGovId);
          }
        });

        console.log(`Found ${docketGovIds.length} dockets in chunk ${this.formatDate(chunk.start)} to ${this.formatDate(chunk.end)}`);
        return docketGovIds;
      } catch (error) {
        console.error(`Error processing date chunk ${this.formatDate(chunk.start)} to ${this.formatDate(chunk.end)}:`, error);
        return [];
      }
    };

    const chunkResults = await this.processTasksWithQueue(chunks, scrapeChunk, 3);
    const allDocketIds = chunkResults.flat();
    const govid_set = new Set(allDocketIds);

    console.log(`Found ${govid_set.size} unique dockets across ${chunks.length} date chunks`);
    return Array.from(govid_set);
  }
}

module.exports = { NyPucScraper, ScrapingMode, RawArtificalPersonType };
