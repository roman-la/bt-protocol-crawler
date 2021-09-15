import scrapy
import re
import math
from xml.etree import ElementTree


class ProtocolCrawler(scrapy.Spider):
    name = 'protocol_crawler'
    base_url = 'https://www.bundestag.de/ajax/filterlist/de/services/opendata/543410-543410'

    def start_requests(self):
        yield scrapy.Request(self.base_url, self.parse)

    # Process base_url to calculate max_offset
    def parse(self, response, **kwargs):
        newest_protocol = response.css('strong::text')[0].get()
        max_offset = int(re.findall('[0-9]+', newest_protocol)[0])
        max_offset = int(math.ceil(max_offset / 10)) * 10
        start_offset = 0
        while start_offset < max_offset:
            yield scrapy.Request(self.base_url + '?offset=' + str(start_offset), self.__process_offset_url)
            start_offset += 10

    def __process_offset_url(self, response):
        for url in response.css('a.bt-link-dokument::attr(href)').getall():
            yield scrapy.Request('https://www.bundestag.de' + url, self.__process_xml)

    @staticmethod
    def __process_xml(response):
        xml = response.body.decode('utf-8')
        root = ElementTree.fromstring(xml)
        session_id = root.find('.//sitzungsnr').text
        yield {'session_id': session_id, 'xml': xml}
