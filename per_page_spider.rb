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
      id: response.xpath("//span[@class='cardMainInfo__purchaseLink distancedText']").text,
      description: response.xpath("//div[@class='sectionMainInfo__body']//span[@class='cardMainInfo__content']").first.text.squish,
      title: response.xpath("//div[@class='sectionMainInfo__body']//span[@class='cardMainInfo__content']").last.text.squish,
      cost: response.xpath("//span[@class='cardMainInfo__content cost']").text.squish,
      from: response.xpath("//div[@class='date']//span[@class='cardMainInfo__content']").first.text.squish,
      to: response.xpath("//div[@class='date']//span[@class='cardMainInfo__content']")[1].text.squish,
      s11: response.xpath("//div[@class='row blockInfo']").first.xpath("//section[@class='blockInfo__section section']//span[@class='section__info']").first.text.squish,
      s12: response.xpath("//div[@class='row blockInfo']").first.xpath("//section[@class='blockInfo__section section']//span[@class='section__info']")[1].text.squish,
      # kontakt?
      s23: response.xpath("//div[@class='row blockInfo']")[1].xpath("//section[@class='blockInfo__section section']//span[@class='section__info']")[2].text.squish,
      s24: response.xpath("//div[@class='row blockInfo']")[1].xpath("//section[@class='blockInfo__section section']//span[@class='section__info']")[3].text.squish,
      s25: response.xpath("//div[@class='row blockInfo']")[1].xpath("//section[@class='blockInfo__section section']//span[@class='section__info']")[4].text.squish,
      s26: response.xpath("//div[@class='row blockInfo']")[1].xpath("//section[@class='blockInfo__section section']//span[@class='section__info']")[5].text.squish,
    }

    save_to "data.csv", item, format: :csv
  end
end

PerPageSpider.crawl!