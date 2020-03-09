создать окружение
```shell script
python3.7 -m venv ~/Projects/envs/cli_scraper
```

обновить pip
```shell script
pip install --upgrade pip
```

активировать окружние
```shell script
source ~/Projects/envs/cli-scraper/bin/activate
```

установить зависимости
```shell script
pip install -r requirements/dev.txt
```

---

запуск скрейпера
```shell script
python -m scraper crawl -f csv -o scraped/csv_file.csv
```

CSS-query в браузере
```javascript
document.querySelector('.pagination-block .pagination .next a::attr(href)')
```
