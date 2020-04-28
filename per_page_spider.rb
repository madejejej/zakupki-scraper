require 'kimurai'
require 'net/http'
require 'mechanize'

#ids = JSON.parse(File.read('ids.json')).map { |a| a['ids'] }.flatten
ids = JSON.parse(File.read('sample2.json'))
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
      procedure_type: value_for_header(response, 'Способ определения поставщика (подрядчика, исполнителя)'),
      published_on: value_for_header(response, 'Адрес электронной площадки в информационно-телекоммуникационной сети \"Интернет\"'),
      publishing_entity: value_for_header(response, 'Организация, осуществляющая размещение'),
      phase: value_for_header(response, 'Этап закупки'),

      # row 2
      procurer_address: value_for_header(response, 'Место нахождения'),
      contact_person: value_for_header(response, 'Ответственное должностное лицо'),
      contact_email: value_for_header(response, 'Адрес электронной почты'),
      contact_phone: value_for_header(response, 'Номер контактного телефона'),

      # row 3
      application_start_date: value_for_header(response, 'Дата и время начала срока подачи заявок'),
      application_end_date: value_for_header(response, 'Дата и время окончания срока подачи заявок на участие в электронном аукционе'),
      auction_date: value_for_header(response, 'Дата проведения аукциона в электронной форме'),

      # not sure about this
      source_of_funding: value_for_header(response, 'Источник финансирования'),

      number_of_lots: (response.xpath("(//h2[@class='blockInfo__title'])[text()='Информация об объекте закупки']/..//span[@class='tableBlock__resultTitle']")[0]&.text&.squish || '')[/\d+/]&.to_i,

      participant_averages: value_for_header(response, 'Преимущества'),
      requirements_towards_participants_number_characters: value_for_header(response, 'Требования к участникам')&.size || 0,
      restrictions_and_bans: value_for_header(response, 'Ограничения и запреты'),

      contract_fullfillment_guarantee: value_for_header(response, 'Размер обеспечения исполнения контракта'),
      application_guarantee: value_for_header(response, 'Размер обеспечения заявки'),
      vadium_amount: value_for_header(response, 'Размер обеспечения гарантийных обязательств')
    }

    save_to "data.csv", item, format: :csv
  end

  def value_for_header(response, header)
    response.xpath("(//span[@class='section__title'])[text()='#{header}']/..//span[@class='section__info']")[0]&.text&.squish
  end
end

PerPageSpider.crawl!