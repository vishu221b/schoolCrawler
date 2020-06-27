from src.impl import BaseCrawlerImpl
import secret


base_crawler = BaseCrawlerImpl()
base = secret.MY_BASE
base_crawler.begin_execution(base)
