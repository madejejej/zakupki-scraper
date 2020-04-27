require 'kimurai'
require 'net/http'
require 'mechanize'

ids = JSON.parse(File.read('ids.json')).map { |a| a['ids'] }.flatten

puts "#{ids.size} IDS"
puts "#{ids.uniq.size} unique IDS"

$ids = ids.uniq

class PerPageSpider < Kimurai::Base
  @name = 'zakupki_spider_per_page'
  @engine = :mechanize
  @config = {
    user_agent: "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:75.0) Gecko/20100101 Firefox/75.0",
    retry_request_errors: [
      Net::ReadTimeout, Net::HTTPServiceUnavailable, Net::HTTPBadGateway, Mechanize::ResponseCodeError, RuntimeError
    ].map { |err| { error: err, skip_on_failure: true }},
    before_request: { delay: 2..5 }
  }
  @start_urls = ['https://zakupki.gov.ru/epz/order/notice/ea44/view/common-info.html?regNumber=0353100010620000006']

  def parse(response, url:, data: {})
    urls = $ids.map do |id|
      "https://zakupki.gov.ru/epz/order/notice/ea44/view/common-info.html?regNumber=#{id}"
    end

    in_parallel(
      :parse_page,
      urls,
      threads: 4
    )
  end

  def parse_page(response, url:, data: {})
    item = {
      url: url,
      tender_number: response.xpath("//span[@class='cardMainInfo__purchaseLink distancedText']").text,
      description: response.xpath("//div[@class='sectionMainInfo__body']//span[@class='cardMainInfo__content']").first.text.squish,
      procurer: response.xpath("//div[@class='sectionMainInfo__body']//span[@class='cardMainInfo__content']").last.text.squish,
      initial_price: response.xpath("//span[@class='cardMainInfo__content cost']").text.squish,
      from: response.xpath("//div[@class='date']//span[@class='cardMainInfo__content']").first.text.squish,
      to: response.xpath("//div[@class='date']//span[@class='cardMainInfo__content']")[1].text.squish,

      # row 1
      procedure_type: response.xpath("(//div[@class='row blockInfo'])[1]//span[@class='section__info']")[0].text.squish,
      published_on: response.xpath("(//div[@class='row blockInfo'])[1]//span[@class='section__info']")[2].text.squish,
      publishing_entity: response.xpath("(//div[@class='row blockInfo'])[1]//span[@class='section__info']")[3].text.squish,
      phase: response.xpath("(//div[@class='row blockInfo'])[1]//span[@class='section__info']")[5].text.squish,

      # row 2
      procurer_address: response.xpath("(//div[@class='row blockInfo'])[2]//span[@class='section__info']")[2].text.squish,
      point_of_contact: response.xpath("(//div[@class='row blockInfo'])[2]//span[@class='section__info']")[3].text.squish,
      contact_email: response.xpath("(//div[@class='row blockInfo'])[2]//span[@class='section__info']")[4].text.squish,
      contact_phone: response.xpath("(//div[@class='row blockInfo'])[2]//span[@class='section__info']")[5].text.squish,

      # row 3
      application_start_date: response.xpath("(//div[@class='row blockInfo'])[3]//span[@class='section__info']")[0].text.squish,
      application_end_date: response.xpath("(//div[@class='row blockInfo'])[3]//span[@class='section__info']")[1].text.squish,
      auction_date: response.xpath("(//div[@class='row blockInfo'])[3]//span[@class='section__info']")[4].text.squish,

      # not sure about this
      source_of_funding: response.xpath("(//div[@class='row blockInfo'])[4]//span[@class='section__info']")[3].text.squish,

      number_of_lots: response.xpath("//span[@class='tableBlock__resultTitle']")[1].text[/\d+/]&.to_i,

      participant_averages: response.xpath("(//div[@class='row blockInfo'])[6]//span[@class='section__info']")[0].text.squish,
      requirements_towards_participants_number_characters: response.xpath("(//div[@class='row blockInfo'])[6]//span[@class='section__info']")[1].text.squish.size,
      restrictions_and_bands: response.xpath("(//div[@class='row blockInfo'])[6]//span[@class='section__info']")[2].text.squish,

      contract_fullfillment_required: response.xpath("(//h2[@class='blockInfo__title'])[text()='Обеспечение исполнения контракта']/..//span[@class='section__info']")[0]&.text&.squish,
      contract_fullfillment_guarantee: response.xpath("(//h2[@class='blockInfo__title'])[text()='Обеспечение исполнения контракта']/..//span[@class='section__info']")[1]&.text&.squish,

      financial_guarantee_required: response.xpath("(//h2[@class='blockInfo__title'])[text()='Обеспечение заявки']/..//span[@class='section__info']")[0]&.text&.squish,
      application_guarantee: response.xpath("(//h2[@class='blockInfo__title'])[text()='Обеспечение заявки']/..//span[@class='section__info']")[1]&.text&.squish,

      vadium_required: response.xpath("(//h2[@class='blockInfo__title'])[text()='Обеспечение гарантийных обязательств']/..//span[@class='section__info']")[0]&.text&.squish,
      vadium_amount: response.xpath("(//h2[@class='blockInfo__title'])[text()='Обеспечение гарантийных обязательств']/..//span[@class='section__info']")[0]&.text&.squish,
    }

    save_to "data.csv", item, format: :csv
  end
end

PerPageSpider.crawl!