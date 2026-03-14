import scrapy
import json
from scraper.items import JobItem


class TimesJobsSpider(scrapy.Spider):
    name = "timesjobs"

    # The secret API endpoint we discovered
    api_url = "https://tjapi.timesjobs.com/search/api/v1/search/jobs/list"

    # Headers to make our spider look like a real browser
    headers = {
        "Content-Type": "application/json",
        "Origin": "https://www.timesjobs.com",
        "Referer": "https://www.timesjobs.com/",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/145.0.0.0 Safari/537.36"
        ),
    }

    def start_requests(self):
        # This replaces start_urls for POST requests
        # We start at page 1
        yield self.make_request(page=1)

    def make_request(self, page):
        # This is the "note" we send to the API
        # Like filling in a search form and submitting it
        payload = {
            "keyword": "\"Data Analytics\",",
            "location": "",
            "experience": "",
            "page": str(page),
            "size": "10",
            "jobFunctions": [],
            "company": "",
            "industry": "",
            "jobFunction": "",
            "functionAreaId": "",
        }

        return scrapy.Request(
            url=self.api_url,
            method="POST",
            headers=self.headers,
            body=json.dumps(payload),
            callback=self.parse,
            # Pass current page number to parse function
            meta={"page": page},
        )

    def parse(self, response):
        # Parse the JSON response
        # Like opening the reply letter and reading it
        data = json.loads(response.text)

        # Get the list of jobs from the response
        jobs = data.get("jobs", [])

        # Get pagination info
        total_pages = int(data.get("totalPages", 0))
        current_page = response.meta["page"]

        self.logger.info(
            f"Page {current_page}/{total_pages} "
            f"— Found {len(jobs)} jobs"
        )

        # Loop through each job in the response
        for job in jobs:
            item = JobItem()

            # --- JOB TITLE ---
            item["job_title"] = job.get("title", "").strip()

            # --- COMPANY NAME ---
            item["company_name"] = job.get("company", "").strip()

            # --- SKILLS ---
            # Skills come as a comma separated string
            # "Python, SQL, Excel" — clean and ready
            item["required_skills"] = job.get("skills", "").strip()

            # --- SALARY ---
            # We have both low and high salary as numbers
            # Let's combine them into a range string
            low = job.get("lowSalary")
            high = job.get("highSalary")
            currency = job.get("currency", "INR")

            if low and high and low != -1 and high != -1:
                item["salary_range"] = (
                    f"{currency} {low} - {high}"
                )
            else:
                item["salary_range"] = "Not disclosed"

            # --- LOCATION ---
            item["location"] = job.get("location", "").strip()

            # --- EXPERIENCE ---
            exp_from = job.get("experienceFrom", "")
            exp_to = job.get("experienceTo", "")

            if exp_from != "" and exp_to != "":
                item["experience_required"] = (
                    f"{exp_from} - {exp_to} Yrs"
                )
            else:
                item["experience_required"] = "Not specified"

            # --- DATE POSTED ---
            item["date_posted"] = job.get("postDate", "").strip()

            # --- JOB URL ---
            item["job_url"] = job.get("jobDetailUrl", "").strip()

            yield item

        # --- PAGINATION ---
        # If there are more pages, request the next one
        # Like turning to the next page of the directory
        if current_page < total_pages:
            yield self.make_request(page=current_page + 1)