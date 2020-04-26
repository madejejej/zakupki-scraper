require 'kimurai'

class ZakupkiSpider < Kimurai::Base
  @name = 'zakupki_spider'
  @engine = :mechanize
  @config = {
    user_agent: "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:75.0) Gecko/20100101 Firefox/75.0",
    #before_request: { delay: 4..7 }
  }
  @start_urls = ['https://zakupki.gov.ru/epz/order/extendedsearch/results.html?morphology=on&search-filter=%D0%94%D0%B0%D1%82%D0%B5+%D1%80%D0%B0%D0%B7%D0%BC%D0%B5%D1%89%D0%B5%D0%BD%D0%B8%D1%8F&sortDirection=false&recordsPerPage=_500&showLotsInfoHidden=false&sortBy=UPDATE_DATE&fz44=on&pc=on&currencyIdGeneral=-1']

  def parse(response, url:, data: {})
    date = Date.parse('2020-04-04')
    last_date = Date.parse('2018-04-04')

    while date > last_date
      date_s = date.strftime('%d.%m.%Y')

      urls = (1..10).to_a.map do |page|
        "#{url}&pageNumber=#{page}&publishDateFrom=#{date_s}&publishDateTo=#{date_s}"
      end

      day_data = { urls: Set.new }

      in_parallel(
        :parse_page,
        urls,
        threads: 8,
        data: day_data
      )

      logger.info "Date: #{date} Collected #{day_data[:urls].size} URLs"

      date = date - 1.day
      item = {
        date: date,
        urls: day_data[:urls].to_a
      }
      save_to "urls.json", item, format: :json
    end
  end

  def parse_page(response, url:, data: {})
    response.xpath("//div[@class='registry-entry__header-mid__number']//a").each do |registry_link|
      data[:urls] << registry_link[:href]
    end
  end
end

ZakupkiSpider.crawl!