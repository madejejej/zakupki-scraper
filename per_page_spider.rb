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

  MULTIPLE_VALUE_LIMIT = 25

  def parse(response, url:, data: {})
    urls = $ids.map do |id|
      "https://zakupki.gov.ru/epz/order/notice/ea44/view/common-info.html?regNumber=#{id}&recordsPerPage=25"
    end

    in_parallel(
      :parse_page,
      urls,
      threads: 8
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
      published_on: value_for_header(response, 'Адрес электронной площадки в информационно-телекоммуникационной сети "Интернет"'),
      publishing_entity:
        value_for_header(response, 'Организация, осуществляющая размещение') ||
          # https://zakupki.gov.ru/epz/order/notice/ep44/view/common-info.html?regNumber=0369300235118000001
          response.xpath("(//span[@class='cardMainInfo__title'])[text()='Заказчик']/..//span[@class='cardMainInfo__content']")[0]&.text&.squish,
      phase: value_for_header(response, 'Этап закупки'),

      # row 2
      procurer_address: value_for_header(response, 'Место нахождения'),
      contact_person: value_for_header(response, 'Ответственное должностное лицо'),
      contact_email: value_for_header(response, 'Адрес электронной почты'),
      contact_phone: value_for_header(response, 'Номер контактного телефона'),

      # row 3
      application_start_date: value_for_header(
        response,
        'Дата и время начала срока подачи заявок',
        'Дата и время начала подачи заявок',
        'Дата и время начала подачи котировочных заявок',
        'Дата и время начала подачи заявок (по местному времени)'
      ),
      application_end_date: value_for_header(
        response,
        'Дата и время окончания срока подачи заявок на участие в электронном аукционе',
        'Дата и время окончания подачи заявок',
        'Дата и время окончания подачи котировочных заявок',
        'Дата и время начала подачи заявок (по местному времени)'
      ),
      auction_date: value_for_header(
        response,
        'Дата проведения аукциона в электронной форме',
        'Дата и время проведения закрытого аукциона',
        'Дата и время вскрытия конвертов с заявками (или) открытия доступа к поданным в',
        'форме электронных документов заявкам на участие в запросе котировок',
        'Дата рассмотрения и оценки заявок'
      ),

      # not sure about this
      source_of_funding: value_for_header(response, 'Источник финансирования'),

      number_of_lots: (response.xpath("(//h2[@class='blockInfo__title'])[text()='Информация об объекте закупки']/..//span[@class='tableBlock__resultTitle']")[0]&.text&.squish || '')[/\d+/]&.to_i,

      participant_averages: value_for_header(response, 'Преимущества'),
      requirements_towards_participants_number_characters: value_for_header(response, 'Требования к участникам')&.size || 0,
      restrictions_and_bans: value_for_header(response, 'Ограничения и запреты')
    }.merge(
      multiple_values(response, 'Размер обеспечения исполнения контракта', :contract_fullfillment_guarantee)
    ).merge(
      multiple_values(response, 'Размер обеспечения заявки', :application_guarantee)
    ).merge(
      multiple_values(response, 'Размер обеспечения гарантийных обязательств', :vadium_amount)
    )

    third_tab = response.xpath("(//a[@class='tabsNav__item'])[text()[contains(., 'Результаты определения поставщика')]]")

    if third_tab.any?
      request_to(:scrape_third_tab, url: absolute_url(third_tab[0][:href], base: url), data: item)
    end

    save_to "data_full.csv", item, format: :csv
  end

  def scrape_third_tab(response, url:, data: {})
    rows = response.xpath("(//div[@class='row blockInfo'][1]//table)[1]/tbody/tr")

    # first row has rowspan, the rest of the rows do not
    first_row = rows[0]

    date_of_decision = first_row.at_xpath('td[5]')&.text&.squish
    participants_and_bids = []
    participants_and_bids << [first_row.at_xpath('td[3]')&.text&.squish, first_row.at_xpath('td[4]')&.text&.squish]

    rows[1..].take(MULTIPLE_VALUE_LIMIT - 1).each do |row|
      participants_and_bids << [row.at_xpath('td[1]')&.text&.squish, row.at_xpath('td[2]')&.text&.squish]
    end

    participants_and_bids_h = (0...MULTIPLE_VALUE_LIMIT).to_a.map do |idx|
      {
        "participant_#{idx + 1}" => participants_and_bids[idx].try(:[], 0),
        "bid_#{idx + 1}" => participants_and_bids[idx].try(:[], 1)
      }
    end.reduce(&:merge)

    data.merge!(participants_and_bids_h)
    data[:date_of_decision] = date_of_decision
  end

  def multiple_values(response, header, field)
    xpath = response.xpath("(//span[@class='section__title'])[text()='#{header}']/..//span[@class='section__info']")

    (0...MULTIPLE_VALUE_LIMIT).to_a.map do |idx|
      ["#{field}_#{idx + 1}", xpath[idx]&.text&.squish]
    end.to_h
  end

  def value_for_header(response, *headers)
    headers.map do |header|
      response.xpath("(//span[@class='section__title'])[text()='#{header}']/..//span[@class='section__info']")[0]&.text&.squish
    end.compact.first
  end
end

PerPageSpider.crawl!