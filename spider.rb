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
  @start_urls = ['https://zakupki.gov.ru/epz/order/extendedsearch/results.html?morphology=on&search-filter=%D0%94%D0%B0%D1%82%D0%B5+%D1%80%D0%B0%D0%B7%D0%BC%D0%B5%D1%89%D0%B5%D0%BD%D0%B8%D1%8F&sortDirection=false&recordsPerPage=_500&showLotsInfoHidden=false&sortBy=PRICE&fz44=on&pc=on&placingWayList=EP44%2CEPP44%2CEA44%2CEAB44%2CZK504%2CZKP504%2CEZK504%2CINM111%2CEP111%2COK504%2COKA504%2CZP504%2CZPP504%2CEZP504%2CZA44%2CZAP44%2CZAE44%2COKU504%2COKUP504%2CEOKU504%2COKUK504%2COKUI504%2COK44%2COKA44%2CZK44%2CZKI44%2CZKK44%2CZKKP44%2CZKKI44%2CZKKE44%2COKD504%2COKDP504%2CEOKD504%2COKDK504%2COKDI504%2CZKKU44%2CZKKUP44%2CZKKUI44%2CZKKUE44%2CZKKD44%2CZKKDP44%2CZKKDI44%2CZKKDE44%2COKU44%2COKUP44%2CPOKU44%2CEOKU44%2COKD44%2COKDP44%2CEOKD44%2CZKB44%2CZKBGP44%2CZP44%2CZPP44%2CEAO44%2CZKOP44%2CZKOO44%2CEOK44%2CZKB111%2CZK111%2COKK504%2COKP44%2CEAP44%2CEEA44%2CZKE44&priceFromGeneral=9785600&currencyIdGeneral=-1&OrderPlacementSmallBusinessSubject=on&OrderPlacementRnpData=on&OrderPlacementExecutionRequirement=on&orderPlacement94_0=0&orderPlacement94_1=0&orderPlacement94_2=0']

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