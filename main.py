from src.impl import BassCrawlerImpl
import secret


base_crawler = BassCrawlerImpl()
base = secret.MY_BASE
base_crawler.begin_execution(base)
