require 'kimurai'
require 'net/http'
require 'mechanize'

require 'kimurai/capybara_ext/session'

module Capybara
  class Session
    attr_accessor :spider

    def visit(visit_uri, delay: config.before_request[:delay], skip_request_options: false, max_retries: 8)
      if spider
        process_delay(delay) if delay
        retries, sleep_interval = 0, 0

        begin
          check_request_options(visit_uri) unless skip_request_options
          driver.requests += 1 and logger.info "Browser: started get request to: #{visit_uri}"
          spider.class.update(:visits, :requests) if spider.with_info

          original_visit(visit_uri)
        rescue => e
          if match_error?(e, type: :to_skip)
            logger.error "Browser: skip request error: #{e.inspect}, url: #{visit_uri}"
            spider.add_event(:requests_errors, e.inspect) if spider.with_info
            false
          elsif match_error?(e, type: :to_retry)
            logger.error "Browser: retry request error: #{e.inspect}, url: #{visit_uri}"
            spider.add_event(:requests_errors, e.inspect) if spider.with_info

            if (retries += 1) <= max_retries
              logger.info "Browser: sleep #{(sleep_interval += 2**(retries + 1) + rand(10))} seconds and process retry № #{retries} to the url: #{visit_uri}"
              sleep sleep_interval and retry
            else
              logger.error "Browser: all retries (#{retries - 1}) to the url #{visit_uri} are gone"
              raise e unless skip_error_on_failure?(e)
            end
          else
            raise e
          end
        else
          driver.responses += 1 and logger.info "Browser: finished get request to: #{visit_uri}"
          spider.class.update(:visits, :responses) if spider.with_info
          driver.visited = true unless driver.visited
          true
        ensure
          if spider.with_info
            logger.info "Info: visits: requests: #{spider.class.visits[:requests]}, responses: #{spider.class.visits[:responses]}"
          end

          if memory = driver.current_memory
            logger.debug "Browser: driver.current_memory: #{memory}"
          end
        end
      else
        original_visit(visit_uri)
      end
    end
  end
end

ids = JSON.parse(File.read('ids_final.json')).map { |a| a['ids'] }.flatten
$ids = ids.uniq

class PerPageSpider < Kimurai::Base
  @name = 'zakupki_spider_per_page'
  @engine = :mechanize
  @config = {
    user_agent: "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:75.0) Gecko/20100101 Firefox/75.0",
    retry_request_errors: [
      Net::ReadTimeout, Net::HTTPServiceUnavailable, Net::HTTPBadGateway, Mechanize::ResponseCodeError, RuntimeError, NoMethodError, Net::HTTP::Persistent::Error, SocketError, Errno::ECONNRESET
    ].map { |err| { error: err, skip_on_failure: true }},
    before_request: { delay: 2..5 }
  }
  @start_urls = ['https://zakupki.gov.ru/epz/order/notice/ea44/view/common-info.html?regNumber=0353100010620000006']

  MULTIPLE_VALUE_LIMIT = 25

  def parse(response, url:, data: {})
    urls = $ids.map do |id|
      "https://zakupki.gov.ru/epz/order/notice/ea44/view/common-info.html?regNumber=#{id}"
    end

    in_parallel(
      :parse_page,
      urls,
      threads: 10
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
      ) || response.xpath("//div[@class='date']//span[@class='cardMainInfo__content']")[1].text.squish,
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
      restrictions_and_bans: value_for_header(response, 'Ограничения и запреты'),
      contract_fullfillment_guarantee_mean: mean_money(response, 'Размер обеспечения исполнения контракта'),
      application_guarantee_mean: mean_money(response, 'Размер обеспечения заявки'),
      vadium_amount_mean: mean_money(response, 'Размер обеспечения гарантийных обязательств')
    }.merge(evaluation_criteria(response))

    # needed in order to have correct headers in the damned CSV
    third_tab_values = ['participant', 'bid'].flat_map do |field|
      (1..MULTIPLE_VALUE_LIMIT).to_a.map do |i|
        { "#{field}_#{i}" => nil }
      end
    end.reduce(&:merge).merge(date_of_decision: nil)

    item.merge!(third_tab_values)

    third_tab = response.xpath("(//a[@class='tabsNav__item'])[text()[contains(., 'Результаты определения поставщика')]]")

    if third_tab.any?
      request_to(:scrape_third_tab, url: absolute_url(third_tab[0][:href], base: url), data: item)
    end

    save_to "data_final_4.json", item, format: :json
  end

  def evaluation_criteria(response)
    next_offset = 1

    response.xpath("((//h2)[text()='Критерии оценки заявок участников']/..//table)[1]/tbody/tr[@class='tableBlock__row']").map.with_index do |row, idx|
      offset = next_offset
      colspan = row.at_xpath("td[#{offset}]").attributes['colspan']

      if colspan
        next_offset = colspan.value.to_i
      else
        next_offset = 1
      end

      {
        "criterion_#{idx + 1}" => row.at_xpath("td[#{offset}]").text.squish,
        "weight_#{idx + 1}" => row.at_xpath("td[#{offset + 1}]").text.squish
      }
    end.reduce(&:merge) || {}
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

  def mean_money(response, header)
    xpath = response.xpath("(//span[@class='section__title'])[text()='#{header}']/..//span[@class='section__info']")

    return if xpath.empty?

    values = xpath.map do |value|
      text = value&.text&.squish

      break if text.nil?

      matches = text.tr(' ', '').tr(',', '.').match(/(\d+\.?\d\d)Российскийрубль/i)

      matches[0].to_f if matches
    end.compact

    values.any? ? values.sum / values.size : 0
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
