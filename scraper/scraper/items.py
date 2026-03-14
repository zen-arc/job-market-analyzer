# items.py
# Think of this as our data collection form.
# Every job we scrape will fill in one copy of this form.

import scrapy

class JobItem(scrapy.Item):
    job_title = scrapy.Field()
    company_name = scrapy.Field()
    required_skills = scrapy.Field()
    salary_range = scrapy.Field()
    location = scrapy.Field()
    experience_required = scrapy.Field()
    job_url = scrapy.Field()
    date_posted = scrapy.Field()