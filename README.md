# ukulele_parser

### Overview
Scraper - простой, синхронный веб-краулер и веб-скрейпер, созданный в обущающих целях
Собирает данные об музыкальных интсрументах, а именно про укулеле

### Install
Устанавливаем зависимости
```shell script
pip install -r requirements/dev.txt
```

### Commands

Запуск краулера
```shell script
python -m scraper crawl [options]
```
&nbsp;&nbsp;&nbsp;&nbsp;Supported options:
  - ```--outfile=FILE``` or ```-o FILE```: файл, в который будут сохранены собранные данные
  - ```--format=FORMAT``` or ```-f FORMAT```: формат файла с собранными данными {csv, jl}
