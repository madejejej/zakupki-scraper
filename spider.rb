require 'kimurai'
require 'net/http'
require 'mechanize'

class ZakupkiSpider < Kimurai::Base
  @name = 'zakupki_spider'
  @engine = :mechanize
  @config = {
    user_agent: "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:75.0) Gecko/20100101 Firefox/75.0",
    retry_request_errors: [
      Net::ReadTimeout, Net::HTTPServiceUnavailable, Net::HTTPBadGateway, Mechanize::ResponseCodeError, RuntimeError
    ].map { |err| { error: err, skip_on_failure: true }},
    before_request: {
      delay: 1..3
    }
  }
  @start_urls = ['https://zakupki.gov.ru/epz/order/extendedsearch/results.html?morphology=on&search-filter=%D0%94%D0%B0%D1%82%D0%B5+%D1%80%D0%B0%D0%B7%D0%BC%D0%B5%D1%89%D0%B5%D0%BD%D0%B8%D1%8F&sortDirection=false&recordsPerPage=_500&showLotsInfoHidden=false&sortBy=UPDATE_DATE&fz44=on&pc=on&currencyIdGeneral=-1']

  def parse(response, url:, data: {})
    date = Date.parse('2020-01-01')
    last_date = Date.parse('2018-01-01')
    all_ids = Set.new

    while date > last_date
      date_s = date.strftime('%d.%m.%Y')

      urls = (1..10).to_a.map do |page|
        "#{url}&pageNumber=#{page}&publishDateFrom=#{date_s}&publishDateTo=#{date_s}"
      end

      day_data = { ids: Set.new }

      in_parallel(
        :parse_page,
        urls,
        threads: 8,
        data: day_data
      )
      day_data[:ids].each { |id| all_ids << id }

      logger.info "Date: #{date} Collected #{day_data[:ids].size} ids"
      logger.info "Collected #{all_ids.size} in total"

      date = date - 1.day
      item = {
        date: date,
        ids: day_data[:ids].to_a
      }
      save_to "ids.json", item, format: :json
    end
  end

  def parse_page(response, url:, data: {})
    response.xpath("//div[@class='registry-entry__header-mid__number']//a").each do |registry_link|
      data[:ids] << registry_link[:href].match(/regNumber=(\d+)/)[1]
    end
  end
end

ZakupkiSpider.crawl!