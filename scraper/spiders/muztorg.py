from parsel import Selector
from requests_html import HTMLSession
from typing import List, Dict, Tuple, Callable


BASE_URL = 'https://www.muztorg.ru'
START_URL = BASE_URL + '/category/ukulele'


def parse(resp: HTMLSession) -> List[Tuple[str, Callable]]:
    """
    Fetch all product links on the page and creates new Crawl Tasks
    :param resp:
    :return List[Tuple[str, Callable]]:
    """
    sel = Selector(resp.html.html)
    product_css = '.product-header .text-uppercase a::attr(href)'
    next_css = '.pagination-block .pagination .next a::attr(href)'
    products = sel.css(product_css).getall()

    if products:
        tasks = [(BASE_URL + url, parse_product) for url in products]
    else:
        tasks = []

    next_page = sel.css(next_css).get()
    if next_page:
        tasks.append((BASE_URL + next_page, parse))

    return tasks


def parse_product(resp: HTMLSession) -> List[Tuple[str, Callable]]:
    """
    Fetch product characteristics link on the page and creates new Crawl Task
    :param resp:
    :return List[Tuple[str, Callable]]:
    """
    sel = Selector(resp.html.html)
    characteristics_css = '#characteristics-tab::attr(href)'
    characteristics_path = sel.css(characteristics_css).get()

    if not characteristics_path:
        return [(resp.url, parse_characteristics)]

    return [(resp.url + characteristics_path, parse_characteristics)]


def parse_characteristics(resp: HTMLSession) -> Dict[str, str]:
    """
    Unpack product characteristics on the page and creates attributes dictionary
    :param resp:
    :return Dict[str, str]:
    """
    sel = Selector(resp.html.html)

    name_css = 'section[itemprop="name"]::text'
    price_css = 'meta[itemprop="price"]::attr(content)'
    availability_css = '#available-informer span::text'

    product = {
        'item_name': sel.css(name_css).get().strip(),
        'price': sel.css(price_css).get().strip(),
        'availability': sel.css(availability_css).get().strip(),
        'color': None,
        'type': None,
        'case': None,
        'range': None,
        'frets': None,
        'body_material': None,
        'ukulele_type': None,
        'fretboard_material': None,
        'fingerboard_material': None,
        'url': resp.url
    }

    characteristics = {
        'Цвет': 'color',
        'Тип': 'type',
        'Чехол/Кейс': 'case',
        'Мензура (диапазон)': 'range',
        'Количество ладов (диапазон)': 'frets',
        'Материал корпуса': 'body_material',
        'Тип корпуса': 'ukulele_type',
        'Материал накладки грифа': 'fretboard_material',
        'Материал грифа': 'fingerboard_material'
    }

    c_css = '#ProductAttributes .panel-body li'
    characteristics_raw = sel.css(c_css).getall()

    if characteristics_raw:
        for characteristic in characteristics_raw:
            c_sel = Selector(characteristic)
            c_key = c_sel.css('b::text').get().strip()
            if not characteristics.get(c_key):
                continue
            c_value = c_sel.css('li::text').get().strip()
            product[characteristics[c_key]] = get_characteristic_text(c_value)

    return [product]


def get_characteristic_text(characteristic_text: str) -> str:
    """
    Return characteristic's text without ': ' symbols
    In: ': Сопрано' Out: 'Сопрано'
    :param characteristic_text:
    :return str:
    """
    return characteristic_text[2:]
