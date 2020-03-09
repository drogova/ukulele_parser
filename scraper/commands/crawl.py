import argparse
from scraper import engine
from typing import NoReturn
from scraper.spiders import muztorg


def execute(args: argparse.Namespace) -> NoReturn:
    """
    Start URL crawler
    :param args:
    :return:
    """
    engine.start(muztorg.START_URL, muztorg.parse, args.outfile, args.format)
