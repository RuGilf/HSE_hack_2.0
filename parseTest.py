#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import json
import logging
import random
import re
import sys
from pathlib import Path
from datetime import datetime
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from typing import Optional
import nodriver as uc


BASE_URL = "https://www.perekrestok.ru"

OUTPUT_DIR = Path("perekrestok_data")
OUTPUT_DIR.mkdir(exist_ok=True)
DEBUG_DIR = OUTPUT_DIR / "debug"
DEBUG_DIR.mkdir(exist_ok=True)

PRODUCTS_FILE = OUTPUT_DIR / "products.json"
PROGRESS_FILE = OUTPUT_DIR / "progress.json"
ERRORS_FILE = OUTPUT_DIR / "errors.json"
LOG_FILE = OUTPUT_DIR / "scraper.log"

HEADLESS = False
PAGE_WAIT_SEC = 10
PRODUCT_WAIT_SEC = 7
CATEGORY_DELAY = (2.0, 4.0)
PRODUCT_DELAY = (1.0, 2.0)
MAX_PAGES_PER_CATEGORY = 80
SAVE_EVERY = 1

CATEGORY_URLS = [
    "https://www.perekrestok.ru/cat/c/284/zelenaa-linia",
    "https://www.perekrestok.ru/cat/c/287/market",
    "https://www.perekrestok.ru/cat/c/290/market-sollection",
    "https://www.perekrestok.ru/cat/c/283/select",
    "https://www.perekrestok.ru/cat/c/322/molocnyj-znak",
    "https://www.perekrestok.ru/cat/c/299/pr-st",
    "https://www.perekrestok.ru/cat/c/1284/ludi-lubat",
    "https://www.perekrestok.ru/cat/c/292/kokoro",
    "https://www.perekrestok.ru/cat/c/32/salaty",
    "https://www.perekrestok.ru/cat/c/30/osnovnye-bluda",
    "https://www.perekrestok.ru/cat/c/1046/premium-menu",
    "https://www.perekrestok.ru/cat/c/1285/kafe-select",
    "https://www.perekrestok.ru/cat/c/306/sendvici",
    "https://www.perekrestok.ru/cat/c/1153/saurma",
    "https://www.perekrestok.ru/cat/c/1154/burgery",
    "https://www.perekrestok.ru/cat/c/298/susi-i-rolly",
    "https://www.perekrestok.ru/cat/c/27/zavtrak",
    "https://www.perekrestok.ru/cat/c/31/pervye-bluda",
    "https://www.perekrestok.ru/cat/c/307/zakuski",
    "https://www.perekrestok.ru/cat/c/26/deserty",
    "https://www.perekrestok.ru/cat/c/320/ostalos-dogotovit",
    "https://www.perekrestok.ru/cat/c/825/kuhni-narodov-mira",
    "https://www.perekrestok.ru/cat/c/319/do-i-posle-edy",
    "https://www.perekrestok.ru/cat/c/28/napitki",
    "https://www.perekrestok.ru/cat/c/1090/sousy",
    "https://www.perekrestok.ru/cat/c/258/nase-kafe",
    "https://www.perekrestok.ru/cat/c/114/moloko",
    "https://www.perekrestok.ru/cat/c/122/syr",
    "https://www.perekrestok.ru/cat/c/117/tvorog",
    "https://www.perekrestok.ru/cat/c/656/syrki",
    "https://www.perekrestok.ru/cat/c/119/jogurty",
    "https://www.perekrestok.ru/cat/c/657/tvorozki",
    "https://www.perekrestok.ru/cat/c/658/deserty-i-sneki",
    "https://www.perekrestok.ru/cat/c/123/ajca",
    "https://www.perekrestok.ru/cat/c/121/maslo",
    "https://www.perekrestok.ru/cat/c/659/margarin",
    "https://www.perekrestok.ru/cat/c/660/spred",
    "https://www.perekrestok.ru/cat/c/120/kislomolocnye-produkty",
    "https://www.perekrestok.ru/cat/c/118/smetana",
    "https://www.perekrestok.ru/cat/c/115/slivki",
    "https://www.perekrestok.ru/cat/c/124/molocnye-konservy",
    "https://www.perekrestok.ru/cat/c/116/molocnye-koktejli",
    "https://www.perekrestok.ru/cat/c/150/ovosi",
    "https://www.perekrestok.ru/cat/c/153/frukty",
    "https://www.perekrestok.ru/cat/c/154/agody",
    "https://www.perekrestok.ru/cat/c/151/zelen-i-salaty",
    "https://www.perekrestok.ru/cat/c/155/griby",
    "https://www.perekrestok.ru/cat/c/149/solena",
    "https://www.perekrestok.ru/cat/c/105/makarony",
    "https://www.perekrestok.ru/cat/c/104/rastitelnoe-maslo",
    "https://www.perekrestok.ru/cat/c/107/krupy",
    "https://www.perekrestok.ru/cat/c/745/bobovye",
    "https://www.perekrestok.ru/cat/c/103/specii-pripravy-i-pranosti",
    "https://www.perekrestok.ru/cat/c/106/muka",
    "https://www.perekrestok.ru/cat/c/102/komponenty-dla-vypecki",
    "https://www.perekrestok.ru/cat/c/101/sol",
    "https://www.perekrestok.ru/cat/c/202/cipsy",
    "https://www.perekrestok.ru/cat/c/774/nacos",
    "https://www.perekrestok.ru/cat/c/709/sneki",
    "https://www.perekrestok.ru/cat/c/710/popkorn",
    "https://www.perekrestok.ru/cat/c/711/suhariki",
    "https://www.perekrestok.ru/cat/c/712/grenki",
    "https://www.perekrestok.ru/cat/c/713/kukuruznye-palocki",
    "https://www.perekrestok.ru/cat/c/199/suski-i-baranki",
    "https://www.perekrestok.ru/cat/c/714/solomki",
    "https://www.perekrestok.ru/cat/c/715/hlebnye-palocki",
    "https://www.perekrestok.ru/cat/c/716/suhari",
    "https://www.perekrestok.ru/cat/c/717/hlebcy",
    "https://www.perekrestok.ru/cat/c/718/vodorosli",
    "https://www.perekrestok.ru/cat/c/197/pecene",
    "https://www.perekrestok.ru/cat/c/702/vafli",
    "https://www.perekrestok.ru/cat/c/703/praniki",
    "https://www.perekrestok.ru/cat/c/195/sokolad",
    "https://www.perekrestok.ru/cat/c/203/batonciki",
    "https://www.perekrestok.ru/cat/c/204/sokoladnye-i-orehovye-pasty",
    "https://www.perekrestok.ru/cat/c/193/konfety",
    "https://www.perekrestok.ru/cat/c/201/torty",
    "https://www.perekrestok.ru/cat/c/704/piroznye",
    "https://www.perekrestok.ru/cat/c/198/pirogi-sdoba-keksy-rulety",
    "https://www.perekrestok.ru/cat/c/705/zefir",
    "https://www.perekrestok.ru/cat/c/191/marmelad",
    "https://www.perekrestok.ru/cat/c/706/pastila",
    "https://www.perekrestok.ru/cat/c/189/diabeticeskie-sladosti",
    "https://www.perekrestok.ru/cat/c/194/ledency",
    "https://www.perekrestok.ru/cat/c/707/draze",
    "https://www.perekrestok.ru/cat/c/188/vostocnye-sladosti-halva",
    "https://www.perekrestok.ru/cat/c/190/zevatelnaa-rezinka",
    "https://www.perekrestok.ru/cat/c/321/morozenoe",
    "https://www.perekrestok.ru/cat/c/208/voda",
    "https://www.perekrestok.ru/cat/c/209/gazirovannye-napitki",
    "https://www.perekrestok.ru/cat/c/211/soki",
    "https://www.perekrestok.ru/cat/c/737/nektary",
    "https://www.perekrestok.ru/cat/c/212/kvas",
    "https://www.perekrestok.ru/cat/c/215/rastitelnye-napitki",
    "https://www.perekrestok.ru/cat/c/210/holodnyj-caj",
    "https://www.perekrestok.ru/cat/c/739/bezalkogolnoe-vino",
    "https://www.perekrestok.ru/cat/c/207/bezalkogolnoe-pivo",
    "https://www.perekrestok.ru/cat/c/213/smuzi",
    "https://www.perekrestok.ru/cat/c/740/sokosoderzasie-napitki",
    "https://www.perekrestok.ru/cat/c/206/energeticeskie-napitki",
    "https://www.perekrestok.ru/cat/c/214/morsy",
    "https://www.perekrestok.ru/cat/c/742/kiseli",
    "https://www.perekrestok.ru/cat/c/743/kompoty",
    "https://www.perekrestok.ru/cat/c/744/sbiten-i-uzvary",
    "https://www.perekrestok.ru/cat/c/216/diabeticeskie-napitki",
    "https://www.perekrestok.ru/cat/c/196/nasa-pekarna",
    "https://www.perekrestok.ru/cat/c/243/hleb",
    "https://www.perekrestok.ru/cat/c/325/pirogi",
    "https://www.perekrestok.ru/cat/c/1152/picca",
    "https://www.perekrestok.ru/cat/c/244/lavas-i-lepeski",
    "https://www.perekrestok.ru/cat/c/245/hlebcy",
    "https://www.perekrestok.ru/cat/c/246/hlebobulocnye-izdelia",
    "https://www.perekrestok.ru/cat/c/1320/vse-maso",
    "https://www.perekrestok.ru/cat/c/138/maso-pticy",
    "https://www.perekrestok.ru/cat/c/951/saslyk",
    "https://www.perekrestok.ru/cat/c/142/govadina",
    "https://www.perekrestok.ru/cat/c/143/krolik",
    "https://www.perekrestok.ru/cat/c/135/polufabrikaty",
    "https://www.perekrestok.ru/cat/c/139/svinina",
    "https://www.perekrestok.ru/cat/c/136/delikatesy-i-kopcenosti",
    "https://www.perekrestok.ru/cat/c/145/fars",
    "https://www.perekrestok.ru/cat/c/141/subprodukty",
    "https://www.perekrestok.ru/cat/c/144/holodcy",
    "https://www.perekrestok.ru/cat/c/778/pastety",
    "https://www.perekrestok.ru/cat/c/781/zalivnoe",
    "https://www.perekrestok.ru/cat/c/779/studni",
    "https://www.perekrestok.ru/cat/c/133/kolbasa",
    "https://www.perekrestok.ru/cat/c/783/vetcina",
    "https://www.perekrestok.ru/cat/c/134/sosiski",
    "https://www.perekrestok.ru/cat/c/784/sardelki",
    "https://www.perekrestok.ru/cat/c/785/spikacki",
    "https://www.perekrestok.ru/cat/c/809/delikatesy-i-kopcenosti",
    "https://www.perekrestok.ru/cat/c/807/holodcy",
    "https://www.perekrestok.ru/cat/c/808/pastety",
    "https://www.perekrestok.ru/cat/c/810/zalivnoe",
    "https://www.perekrestok.ru/cat/c/811/studni",
    "https://www.perekrestok.ru/cat/c/812/vsa-ryba",
    "https://www.perekrestok.ru/cat/c/175/solenaa-marinovannaa-ryba",
    "https://www.perekrestok.ru/cat/c/176/ohlazdennaa-ryba",
    "https://www.perekrestok.ru/cat/c/273/zamorozennaa-ryba",
    "https://www.perekrestok.ru/cat/c/177/kopcenaa-ryba",
    "https://www.perekrestok.ru/cat/c/181/rybnye-konservy-i-kulinaria",
    "https://www.perekrestok.ru/cat/c/794/valenaa-ryba",
    "https://www.perekrestok.ru/cat/c/178/susenaa-ryba",
    "https://www.perekrestok.ru/cat/c/184/rybnye-preservy",
    "https://www.perekrestok.ru/cat/c/186/sobstvennoe-proizvodstvo",
    "https://www.perekrestok.ru/cat/c/58/picca-vareniki-pelmeni-bliny",
    "https://www.perekrestok.ru/cat/c/55/moreprodukty",
    "https://www.perekrestok.ru/cat/c/59/ovosi-i-smesi",
    "https://www.perekrestok.ru/cat/c/56/zamorozennye-polufabrikaty",
    "https://www.perekrestok.ru/cat/c/57/ryba",
    "https://www.perekrestok.ru/cat/c/60/kotlety-naggetsy",
    "https://www.perekrestok.ru/cat/c/61/agody-i-frukty",
    "https://www.perekrestok.ru/cat/c/63/maso-ptica-i-subprodukty",
    "https://www.perekrestok.ru/cat/c/62/zamorozennye-deserty",
    "https://www.perekrestok.ru/cat/c/305/led",
    "https://www.perekrestok.ru/cat/c/180/ikra",
    "https://www.perekrestok.ru/cat/c/182/krabovoe-maso-i-palocki",
    "https://www.perekrestok.ru/cat/c/179/krevetki",
    "https://www.perekrestok.ru/cat/c/787/midii",
    "https://www.perekrestok.ru/cat/c/786/kalmary",
    "https://www.perekrestok.ru/cat/c/788/koktejli-iz-moreproduktov",
    "https://www.perekrestok.ru/cat/c/789/ustricy",
    "https://www.perekrestok.ru/cat/c/792/preservy-iz-moreproduktov",
    "https://www.perekrestok.ru/cat/c/276/voda-i-napitki",
    "https://www.perekrestok.ru/cat/c/230/detskoe-pitanie",
    "https://www.perekrestok.ru/cat/c/277/detskie-smesi-i-zameniteli",
    "https://www.perekrestok.ru/cat/c/231/gigiena-i-uhod",
    "https://www.perekrestok.ru/cat/c/234/detskaa-odezda-i-aksessuary",
    "https://www.perekrestok.ru/cat/c/233/detskaa-posuda",
    "https://www.perekrestok.ru/cat/c/232/igruski",
    "https://www.perekrestok.ru/cat/c/279/razvivausie-igruski-i-konstruktory",
    "https://www.perekrestok.ru/cat/c/280/nastolnye-igry",
    "https://www.perekrestok.ru/cat/c/278/igrovye-nabory",
    "https://www.perekrestok.ru/cat/c/281/nabory-dla-tvorcestva",
    "https://www.perekrestok.ru/cat/c/218/sousy",
    "https://www.perekrestok.ru/cat/c/221/majonez",
    "https://www.perekrestok.ru/cat/c/219/ketcupy-i-tomatnye-sousy",
    "https://www.perekrestok.ru/cat/c/220/tomatnaa-pasta",
    "https://www.perekrestok.ru/cat/c/223/gorcica",
    "https://www.perekrestok.ru/cat/c/746/hren",
    "https://www.perekrestok.ru/cat/c/222/uksus",
    "https://www.perekrestok.ru/cat/c/80/kofe",
    "https://www.perekrestok.ru/cat/c/82/caj",
    "https://www.perekrestok.ru/cat/c/83/sahar",
    "https://www.perekrestok.ru/cat/c/81/kakao",
    "https://www.perekrestok.ru/cat/c/766/goracij-sokolad",
    "https://www.perekrestok.ru/cat/c/225/hlopa",
    "https://www.perekrestok.ru/cat/c/747/musli",
    "https://www.perekrestok.ru/cat/c/748/podusecki",
    "https://www.perekrestok.ru/cat/c/749/sariki",
    "https://www.perekrestok.ru/cat/c/750/kolecki",
    "https://www.perekrestok.ru/cat/c/751/figurki",
    "https://www.perekrestok.ru/cat/c/752/granola",
    "https://www.perekrestok.ru/cat/c/754/ris",
    "https://www.perekrestok.ru/cat/c/172/kasi",
    "https://www.perekrestok.ru/cat/c/755/kasi-bystrogo-prigotovlenia",
    "https://www.perekrestok.ru/cat/c/170/lapsa",
    "https://www.perekrestok.ru/cat/c/169/supy",
    "https://www.perekrestok.ru/cat/c/171/pure",
    "https://www.perekrestok.ru/cat/c/756/kasi",
    "https://www.perekrestok.ru/cat/c/173/vtorye-bluda",
    "https://www.perekrestok.ru/cat/c/76/ovosnye-konservy",
    "https://www.perekrestok.ru/cat/c/75/rybnye-konservy",
    "https://www.perekrestok.ru/cat/c/78/masnye-konservy",
    "https://www.perekrestok.ru/cat/c/77/fruktovye-konservy",
    "https://www.perekrestok.ru/cat/c/24/diabeticeskaa-produkcia",
    "https://www.perekrestok.ru/cat/c/65/sportivnoe-pitanie-i-bad",
    "https://www.perekrestok.ru/cat/c/251/bez-glutena",
    "https://www.perekrestok.ru/cat/c/252/superfud",
    "https://www.perekrestok.ru/cat/c/253/poleznyj-perekus",
    "https://www.perekrestok.ru/cat/c/254/rastitelnye-napitki",
    "https://www.perekrestok.ru/cat/c/255/pravilnye-sladosti",
    "https://www.perekrestok.ru/cat/c/257/napitki-s-polzoj",
    "https://www.perekrestok.ru/cat/c/1676/funkcionalnye-produkty",
    "https://www.perekrestok.ru/cat/c/159/orehi",
    "https://www.perekrestok.ru/cat/c/161/semecki",
    "https://www.perekrestok.ru/cat/c/158/smesi-orehov-i-suhofruktov",
    "https://www.perekrestok.ru/cat/c/160/suhofrukty",
    "https://www.perekrestok.ru/cat/c/112/med",
    "https://www.perekrestok.ru/cat/c/111/varene",
    "https://www.perekrestok.ru/cat/c/109/dzem",
    "https://www.perekrestok.ru/cat/c/775/konfitur",
    "https://www.perekrestok.ru/cat/c/110/siropy",
    "https://www.perekrestok.ru/cat/c/67/dla-kosek",
    "https://www.perekrestok.ru/cat/c/68/dla-sobak",
    "https://www.perekrestok.ru/cat/c/69/dla-gryzunov",
    "https://www.perekrestok.ru/cat/c/72/dla-ptic",
    "https://www.perekrestok.ru/cat/c/297/apteka",
    "https://www.perekrestok.ru/cat/c/97/bumaznaa-i-vatnaa-produkcia",
    "https://www.perekrestok.ru/cat/c/90/uhod-za-polostu-rta",
    "https://www.perekrestok.ru/cat/c/94/sredstva-licnoj-gigieny",
    "https://www.perekrestok.ru/cat/c/91/uhod-dla-volos",
    "https://www.perekrestok.ru/cat/c/87/stajling-volos",
    "https://www.perekrestok.ru/cat/c/85/mylo",
    "https://www.perekrestok.ru/cat/c/86/geli-dla-dusa",
    "https://www.perekrestok.ru/cat/c/92/dezodoranty",
    "https://www.perekrestok.ru/cat/c/95/sredstva-dla-brita",
    "https://www.perekrestok.ru/cat/c/89/uhod-za-licom",
    "https://www.perekrestok.ru/cat/c/88/uhod-za-telom",
    "https://www.perekrestok.ru/cat/c/99/prezervativy-smazki",
    "https://www.perekrestok.ru/cat/c/93/uhod-za-rukami",
    "https://www.perekrestok.ru/cat/c/96/kosmeticeskie-nabory",
    "https://www.perekrestok.ru/cat/c/98/gubki-mocalki-dla-dusa",
    "https://www.perekrestok.ru/cat/c/275/ekodom",
    "https://www.perekrestok.ru/cat/c/237/dla-stirki-i-uhoda-za-vesami",
    "https://www.perekrestok.ru/cat/c/259/dla-myta-posudy",
    "https://www.perekrestok.ru/cat/c/262/dla-santehniki",
    "https://www.perekrestok.ru/cat/c/264/universalnye-sredstva",
    "https://www.perekrestok.ru/cat/c/261/dla-posudomoecnyh-i-stiralnyh-masin",
    "https://www.perekrestok.ru/cat/c/263/dla-ustranenia-zasorov",
    "https://www.perekrestok.ru/cat/c/265/dla-plit-i-duhovok",
    "https://www.perekrestok.ru/cat/c/267/dla-stekol-i-zerkal",
    "https://www.perekrestok.ru/cat/c/268/dla-polov",
    "https://www.perekrestok.ru/cat/c/266/dla-mebeli-i-kovrov",
    "https://www.perekrestok.ru/cat/c/241/predmety-dla-uborki",
    "https://www.perekrestok.ru/cat/c/239/aromatizatory-dla-doma",
    "https://www.perekrestok.ru/cat/c/240/uhod-za-odezdoj-i-obuvu",
    "https://www.perekrestok.ru/cat/c/163/posuda-dla-prigotovlenia",
    "https://www.perekrestok.ru/cat/c/165/odnorazovaa-posuda",
    "https://www.perekrestok.ru/cat/c/166/servirovka",
    "https://www.perekrestok.ru/cat/c/164/kruzki-stakany-bokaly",
    "https://www.perekrestok.ru/cat/c/36/vse-dla-hranenia",
    "https://www.perekrestok.ru/cat/c/43/odezda-obuv-aksessuary",
    "https://www.perekrestok.ru/cat/c/38/vse-dla-daci-i-sada",
    "https://www.perekrestok.ru/cat/c/47/meloci-dla-doma",
    "https://www.perekrestok.ru/cat/c/46/tehnika-i-aksessuary",
    "https://www.perekrestok.ru/cat/c/42/vse-dla-prazdnika",
    "https://www.perekrestok.ru/cat/c/37/lampocki-i-batarejki",
    "https://www.perekrestok.ru/cat/c/41/vse-dla-saslyka",
    "https://www.perekrestok.ru/cat/c/44/domasnij-tekstil",
    "https://www.perekrestok.ru/cat/c/48/avtoaksessuary",
    "https://www.perekrestok.ru/cat/c/45/dekor-i-interer",
    "https://www.perekrestok.ru/cat/c/51/sport-i-turizm",
    "https://www.perekrestok.ru/cat/c/40/galanterejnye-aksessuary",
    "https://www.perekrestok.ru/cat/c/52/tovary-dla-bani-i-sauny",
    "https://www.perekrestok.ru/cat/c/53/kancelaria",
    "https://www.perekrestok.ru/cat/c/1308/pressa-knigi",
    "https://www.perekrestok.ru/cat/c/2/vino",
    "https://www.perekrestok.ru/cat/c/3/igristye-vina",
    "https://www.perekrestok.ru/cat/c/758/sampanskoe",
    "https://www.perekrestok.ru/cat/c/4/konak",
    "https://www.perekrestok.ru/cat/c/5/viski",
    "https://www.perekrestok.ru/cat/c/759/burbon",
    "https://www.perekrestok.ru/cat/c/15/rom",
    "https://www.perekrestok.ru/cat/c/6/vodka",
    "https://www.perekrestok.ru/cat/c/760/absent",
    "https://www.perekrestok.ru/cat/c/761/samogon",
    "https://www.perekrestok.ru/cat/c/9/pivo",
    "https://www.perekrestok.ru/cat/c/7/sidr",
    "https://www.perekrestok.ru/cat/c/762/medovuha",
    "https://www.perekrestok.ru/cat/c/13/tekila",
    "https://www.perekrestok.ru/cat/c/8/nastojki",
    "https://www.perekrestok.ru/cat/c/10/dzin",
    "https://www.perekrestok.ru/cat/c/14/brendi",
    "https://www.perekrestok.ru/cat/c/16/likery",
    "https://www.perekrestok.ru/cat/c/764/vermut",
    "https://www.perekrestok.ru/cat/c/17/balzam",
    "https://www.perekrestok.ru/cat/c/11/slaboalkogolnye-napitki",
    "https://www.perekrestok.ru/cat/c/250/stiki",
    "https://www.perekrestok.ru/cat/c/249/ustrojstva",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ],
)
log = logging.getLogger("perekrestok")


def normalize_url(url: str) -> str:
    if url.startswith("/"):
        return BASE_URL + url
    return url


def safe_name(text: str) -> str:
    return re.sub(r"[^\w\-]+", "_", text)[:80]


def set_page_param(url: str, page_num: int) -> str:
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)
    qs["page"] = [str(page_num)]
    new_query = urlencode(qs, doseq=True)
    return urlunparse((
        parsed.scheme,
        parsed.netloc,
        parsed.path,
        parsed.params,
        new_query,
        parsed.fragment,
    ))


class PerekrestokNodriverScraper:
    def __init__(self):
        self.browser = None
        self.products = {}
        self.seen_urls = set()
        self.errors = []
        self.save_counter = 0
        self._load_progress()

    def _load_progress(self):
        if PRODUCTS_FILE.exists():
            try:
                data = json.loads(PRODUCTS_FILE.read_text(encoding="utf-8"))
                for item in data:
                    url = item.get("url")
                    if url:
                        self.products[url] = item
                        self.seen_urls.add(url)
                log.info(f"Загружен прогресс: {len(self.products)} товаров")
            except Exception as e:
                log.warning(f"Не удалось загрузить products.json: {e}")

        if ERRORS_FILE.exists():
            try:
                self.errors = json.loads(ERRORS_FILE.read_text(encoding="utf-8"))
            except Exception:
                self.errors = []

    def _save(self, force=False):
        self.save_counter += 1
        if not force and self.save_counter % SAVE_EVERY != 0:
            return

        PRODUCTS_FILE.write_text(
            json.dumps(list(self.products.values()), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        PROGRESS_FILE.write_text(
            json.dumps(
                {
                    "total_products": len(self.products),
                    "total_errors": len(self.errors),
                    "updated_at": datetime.now().isoformat(),
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        ERRORS_FILE.write_text(
            json.dumps(self.errors, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    async def start_browser(self):
        self.browser = await uc.start(
            headless=HEADLESS,
            browser_args=[
                "--start-maximized",
                "--disable-blink-features=AutomationControlled",
            ],
        )
        log.info("Браузер запущен")

    async def stop_browser(self):
        if self.browser:
            try:
                await self.browser.stop()
            except Exception:
                pass

    async def save_debug_html(self, page, name: str):
        try:
            html = await page.get_content()
            (DEBUG_DIR / f"{safe_name(name)}.html").write_text(html, encoding="utf-8")
        except Exception as e:
            log.warning(f"Не удалось сохранить debug html {name}: {e}")

    async def ensure_site_access(self):
        log.info("Открываю главную, чтобы пройти антибот")
        page = await self.browser.get(BASE_URL)
        await asyncio.sleep(15)

        html = await page.get_content()
        if "servicepipe" in html.lower() or "forbidden" in html.lower():
            await self.save_debug_html(page, "homepage_antibot")
            log.warning("Антибот активен. Дождись в браузере нормальной загрузки сайта.")
            input("Когда сайт загрузится нормально, нажми Enter...")
            await asyncio.sleep(3)

        html = await page.get_content()
        await self.save_debug_html(page, "homepage_after")
        if "servicepipe" in html.lower() or "forbidden" in html.lower():
            raise RuntimeError("Антибот не пройден.")
        log.info("Доступ к сайту подтвержден")

    async def collect_links_from_current_page(self, page) -> list[str]:
            # 1. Плавный скроллинг страницы вниз, чтобы React отрендерил все карточки (Lazy Load)
            for _ in range(4):
                try:
                    await page.scroll_down(800)
                    await asyncio.sleep(1.5)
                except Exception:
                    pass
            
            # Еще немного подождем финальной отрисовки
            await asyncio.sleep(2)

            # 2. Выполняем JS. Превращаем результат в JSON-строку (JSON.stringify)
            links_json = await page.evaluate("""
                (() => {
                    let result = [];
                    // Ищем по классам из вашего HTML или просто по наличию /p/ в href
                    let elements = document.querySelectorAll('a.product-card__link, a.product-card__title-link, a[href*="/p/"]');
                    
                    for (let el of elements) {
                        let href = el.getAttribute('href');
                        if (href && href.includes('/p/')) {
                            result.push(href);
                        }
                    }
                    return JSON.stringify(result);
                })()
            """)

            # 3. Распаковываем JSON и обрабатываем ссылки
            out = set()
            if links_json:
                try:
                    links_data = json.loads(links_json)
                    for href in links_data:
                        # Теперь href - это гарантированно строка
                        out.add(normalize_url(href))
                except Exception as e:
                    log.error(f"Ошибка при чтении ссылок: {e}")

            return sorted(list(out))





    async def collect_category_links(self, category_url: str) -> list[str]:
        all_links = set()

        first_page = await self.browser.get(category_url)
        
        # Ждем загрузки именно карточек товаров вместо слепого sleep
        try:
            await first_page.select('.product-card', timeout=15)
        except Exception:
            log.warning("Не дождались появления .product-card на первой странице, ждем еще...")
            await asyncio.sleep(PAGE_WAIT_SEC)

        html = await first_page.get_content()
        if "servicepipe" in html.lower() or "forbidden" in html.lower():
            await self.save_debug_html(first_page, f"blocked_{category_url}")
            raise RuntimeError(f"Антибот на категории: {category_url}")

        real_url = first_page.url
        log.info(f"Реальный URL категории после редиректа: {real_url}")

        page1_links = await self.collect_links_from_current_page(first_page)
        if page1_links:
            all_links.update(page1_links)
            log.info(f"Страница 1: найдено {len(page1_links)} ссылок")
        else:
            await self.save_debug_html(first_page, f"empty_first_{real_url}")
            log.warning("На первой странице товары не найдены (возможно, не прогрузились)")

        base_for_pagination = real_url
        parsed = urlparse(base_for_pagination)
        qs = parse_qs(parsed.query)
        if "page" in qs:
            qs.pop("page", None)
            base_for_pagination = urlunparse((
                parsed.scheme,
                parsed.netloc,
                parsed.path,
                parsed.params,
                urlencode(qs, doseq=True),
                parsed.fragment,
            ))

        for page_num in range(2, MAX_PAGES_PER_CATEGORY + 1):
            page_url = set_page_param(base_for_pagination, page_num)
            log.info(f"Проверяю страницу {page_num}: {page_url}")

            page = await self.browser.get(page_url)
            
            # Явное ожидание карточек при пагинации
            try:
                await page.select('.product-card', timeout=10)
            except Exception:
                await asyncio.sleep(PAGE_WAIT_SEC)

            html = await page.get_content()
            if "servicepipe" in html.lower() or "forbidden" in html.lower():
                await self.save_debug_html(page, f"blocked_page_{page_num}_{page_url}")
                log.warning(f"Антибот на page={page_num}, останавливаю категорию")
                break

            links = await self.collect_links_from_current_page(page)
            new_links = set(links) - all_links

            log.info(
                f"Страница {page_num}: найдено {len(links)} ссылок, новых {len(new_links)}"
            )

            # Если сайт вернул пустую страницу товаров или нас перекинуло на 1 страницу
            if not links or not new_links:
                break

            all_links.update(new_links)

        return sorted(list(all_links))

    async def scrape_product(self, product_url: str) -> Optional[dict]:
            try:
                page = await self.browser.get(product_url)
            except Exception as e:
                log.error(f"Ошибка при загрузке URL {product_url}: {e}")
                return None

            # Мягкое ожидание отрисовки React (до 10 секунд)
            h1_found = False
            for _ in range(5):
                await asyncio.sleep(2)  # Ждем отрисовки
                try:
                    # Проверяем, появился ли h1 через JS
                    has_h1 = await page.evaluate("""
                        !!(document.querySelector('h1[itemprop="name"]') || document.querySelector('h1.product__title') || document.querySelector('h1'))
                    """)
                    if has_h1:
                        h1_found = True
                        break
                except Exception:
                    pass

            if not h1_found:
                # Проверяем, не вылез ли антибот
                html = await page.get_content()
                if "servicepipe" in html.lower() or "forbidden" in html.lower():
                    log.warning("Антибот на карточке товара!")
                    return None
                else:
                    log.warning(f"Пропуск: Товар не найден или снят с продажи ({product_url})")
                    return None

            # Плавный скролл вниз, чтобы подгрузить скрытые блоки (состав, КБЖУ)
            for _ in range(3):
                try:
                    await page.scroll_down(700)
                    await asyncio.sleep(1)
                except Exception:
                    pass

            # Выполняем парсинг внутри браузера и собираем JSON
            product_json = await page.evaluate("""
                (() => {
                    const result = {
                        name: null,
                        weight: null,
                        price: null,
                        nutrition: null,
                        composition: null
                    };

                    // 1. Название и граммовки
                    const h1 = document.querySelector('h1[itemprop="name"]') || document.querySelector('h1.product__title') || document.querySelector('h1');
                    if (h1) {
                        result.name = h1.textContent.trim();
                        
                        // Вытаскиваем вес/объем из конца названия (например: ", 180г" или " 1 кг")
                        const weightMatch = result.name.match(/,\\s*([\\d\\.,]+\\s*(г|кг|мл|л|шт\\.?))$/i);
                        if (weightMatch) {
                            result.weight = weightMatch[1];
                        }
                    }

                    // 2. Цена
                    const priceMeta = document.querySelector('meta[itemprop="price"]');
                    if (priceMeta) {
                        result.price = parseFloat(priceMeta.getAttribute('content'));
                    } else {
                        const priceEl = document.querySelector('.price-new');
                        if (priceEl) {
                            let text = priceEl.textContent.replace('Цена', '').replace('₽', '').replace(',', '.').replace(/\\s/g, '').trim();
                            result.price = parseFloat(text);
                        }
                    }

                    // 3. КБЖУ (Пищевая ценность)
                    const nutrition = {};
                    document.querySelectorAll('.product-calories-item').forEach(item => {
                        const t = item.querySelector('.product-calories-item__title');
                        const v = item.querySelector('.product-calories-item__value');
                        if (t && v) {
                            nutrition[t.textContent.trim()] = v.textContent.trim();
                        }
                    });
                    if (Object.keys(nutrition).length > 0) {
                        result.nutrition = nutrition;
                    }

                    // 4. Состав
                    const headers = Array.from(document.querySelectorAll('h2, div[font-weight="600"]'));
                    const compTitle = headers.find(el => el.textContent.trim().includes('Состав'));
                    
                    if (compTitle) {
                        const parent = compTitle.parentElement;
                        const p = parent ? parent.querySelector('p') : null;
                        if (p && p.textContent.trim().length > 5) {
                            result.composition = p.textContent.trim();
                        } else if (compTitle.nextElementSibling && compTitle.nextElementSibling.tagName === 'P') {
                            result.composition = compTitle.nextElementSibling.textContent.trim();
                        }
                    }

                    return JSON.stringify(result);
                })()
            """)

            if not product_json:
                return None

            try:
                data = json.loads(product_json)
            except Exception as e:
                log.error(f"Ошибка декодирования JSON товара: {e}")
                return None

            return {
                "url": product_url,
                "name": data.get("name"),
                "weight": data.get("weight"),
                "price": data.get("price"),
                "nutrition": data.get("nutrition"),
                "composition": data.get("composition"),
                "scraped_at": datetime.now().isoformat()
            }

    async def run(self):
        await self.start_browser()
        try:
            await self.ensure_site_access()

            log.info(f"Всего категорий: {len(CATEGORY_URLS)}")
            log.info(f"Уже собрано: {len(self.products)} товаров")

            for idx, category_url in enumerate(CATEGORY_URLS, start=1):
                log.info("=" * 80)
                log.info(f"Категория {idx}/{len(CATEGORY_URLS)}: {category_url}")

                try:
                    links = await self.collect_category_links(category_url)
                    log.info(f"Всего ссылок в категории: {len(links)}")

                    new_links = [x for x in links if x not in self.seen_urls]
                    log.info(f"Новых товаров: {len(new_links)}")

                    for p_idx, product_url in enumerate(new_links, start=1):
                        log.info(f"[{p_idx}/{len(new_links)}] {product_url}")
                        try:
                            product = await self.scrape_product(product_url)
                            if product and product.get("name"):
                                self.products[product_url] = product
                                self.seen_urls.add(product_url)
                                log.info(
                                    f"✓ {product['name'][:70]} | "
                                    f"КБЖУ={'да' if product.get('nutrition') else 'нет'} | "
                                    f"Состав={'да' if product.get('composition') else 'нет'}"
                                )
                            else:
                                self.errors.append({
                                    "url": product_url,
                                    "error": "empty product or blocked",
                                    "time": datetime.now().isoformat(),
                                })
                                log.warning("Не удалось извлечь данные товара")
                        except Exception as e:
                            self.errors.append({
                                "url": product_url,
                                "error": str(e),
                                "time": datetime.now().isoformat(),
                            })
                            log.error(f"Ошибка товара: {e}")

                        self._save()
                        await asyncio.sleep(random.uniform(*PRODUCT_DELAY))

                except Exception as e:
                    self.errors.append({
                        "url": category_url,
                        "error": str(e),
                        "time": datetime.now().isoformat(),
                    })
                    log.error(f"Ошибка категории: {e}")

                self._save(force=True)
                await asyncio.sleep(random.uniform(*CATEGORY_DELAY))

        except KeyboardInterrupt:
            log.info("Остановлено пользователем")
        finally:
            self._save(force=True)
            await self.stop_browser()
            log.info(f"Готово. Всего товаров: {len(self.products)}")


async def main():
    scraper = PerekrestokNodriverScraper()
    await scraper.run()


if __name__ == "__main__":
    asyncio.run(main())