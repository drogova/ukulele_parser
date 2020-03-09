import argparse
from typing import NoReturn
from scraper.commands import crawl
from scraper.engine import FORMAT_CSV, FORMAT_JL, SIGN_STDOUT


def parse() -> NoReturn:
    """
    Parse cmdline arguments
    """
    parser = argparse.ArgumentParser(prog='scraper')
    subparsers = parser.add_subparsers()

    parser_crawl = subparsers.add_parser('crawl')
    parser_crawl.add_argument('-o', '--outfile', metavar='FILE', default=SIGN_STDOUT)
    parser_crawl.add_argument('-f', '--format', default=FORMAT_CSV, choices=[FORMAT_CSV, FORMAT_JL])
    parser_crawl.set_defaults(func=crawl.execute)

    args = parser.parse_args()
    args.func(args)
