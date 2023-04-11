from datetime import datetime
import logging
import shutil
import xml.etree.cElementTree as ET
import pandas as pd
from pathlib import Path
from sys import argv
from chardet.universaldetector import UniversalDetector

def encoding_file(v_path): # Получение кодировки исходного файла
    detector = UniversalDetector()
    with open(v_path, 'rb') as fh:
        for line in fh:
            detector.feed(line)
            if detector.done:
                break
        detector.close()
    return detector.result.get('encoding')

def ways_to_folder(v_path): # получение директории в которой необходимо создавать файлы и подпапки
    directory = v_path.split('\\')
    name_file = directory[-1:]
    path_to_directory = ','.join(directory[:-1])
    path_to_directory = path_to_directory.replace(',', '\\')
    return path_to_directory

def log_settings(x, ways_to_folder):
    # Проверка на то существует ли подпапка с название log
    if not x.joinpath('log').is_dir():
        x.joinpath('log').mkdir()

    # настройка файла лога
    logging.basicConfig(level=logging.INFO, filename=ways_to_folder + "\log\py_log.log",filemode="w")

def xml_to_csv(v_path, encoding):
    # Проверка файла на принадлежность к формату xml
    v_path = Path(v_path)
    if v_path.suffixes == ['.xml']:

        # Разбор Xml файла
        tree = ET.parse(v_path)
        root = tree.getroot()
        payers_sp = []

        file_name = v_path.stem # имя файла для первого поля
        file_date = root.find('СлЧаст/ОбщСвСч/ИдФайл/ДатаФайл').text # дата для второго поля
        try: # проверка даты на принадлежность к формату
            datetime.strptime(file_date, '%d.%m.%Y')
        except ValueError:
            logging.info('Внимание! Дата актуальности данных не верного формата.')
        common_data = [file_name, file_date] # список для последующего добавление в csv файл

        chek_perid_personal_account = [] # список для исключения не уникальных пар ЛицСч + Период

        # За одну терацию мы проходимся по одному плательщику и фильтруем данные
        for payer in root.iter('Плательщик'):
            sp = [] # список для сбора информации по плательщикам
            personal_account = payer.find('ЛицСч').text # Лицевой счет
            full_name = payer.find('ФИО').text # Полное имя
            address = payer.find('Адрес').text # Адрес
            sp.extend([personal_account, full_name, address]) # Запись в список

            period = payer.find('Период').text # Период
            try:
                # Проверка графы Period на принадлежность к формату
                datetime.strptime(period, '%m%Y')
                sp.append(period)
            except ValueError:
                logging.info(f'Невозможно загрузить строку Период {period} неверного формата {full_name}')

            sum = payer.find('Сумма').text # Сумма
            try:
                # Проверка графы сумма на положительное число float
                if float(sum) >= 0:
                    sp.append(sum)
                else:
                    raise ValueError
            except ValueError :
                logging.info(f'Поле Сумма неверного формата или меньше нуля. {sum}, {full_name}')
                pass
            # Исключение записи в последующий список всех элементов не удовлетворящих условию
            if len(sp) == 5:
                payers_sp.append(sp)
            else:
                continue

            # Запись в список пар ЛицСч + Период
            chek_perid_personal_account.append(period + personal_account)

        # Определение и логирование повторяющихся пар ЛицСч + Период
        inndex_delete = []
        for i in chek_perid_personal_account:
            result = chek_perid_personal_account.count(i)
            inndex_delete.append(result)
            if result > 1:
                logging.warning(f' Лицевой счет {payers_sp[chek_perid_personal_account.index(i)][:1]}'
                      f' и период {payers_sp[chek_perid_personal_account.index(i)][3:4]} не уникальны')

        # Удаление строк по индексу встречающихся более 1 раза записей ЛицСч + Период
        indices = [i for i in range(len(inndex_delete)) if inndex_delete[i] != 1]
        for i in reversed(indices):
            payers_sp.pop(i)

        # Добавить ко всем нужным записям инфу о файле
        payers_sp = [common_data + i for i in payers_sp]


        # Создание DataFrame для записи в csv файл
        df = pd.DataFrame(payers_sp)

        # Создание и запись в csv файл
        df.to_csv(f'{v_path.parent.joinpath(v_path.stem)}.csv', sep=';', index=False, header=False, encoding=encoding)

        # Перемещение обрабатываемого файла в подпапку Arh
        if not v_path.parent.joinpath('arh').is_dir():
            v_path.parent.joinpath('arh').mkdir()
        shutil.move(v_path, v_path.parent.joinpath('arh'))

        #Перемещение файла в папку bad при условии что он не является xml файлом
    else:
        logging.warning(f' file {v_path} is not xml')
        if not v_path.parent.joinpath('bad').is_dir():
            v_path.parent.joinpath('bad').mkdir()
        shutil.move(v_path, v_path.parent.joinpath('bad'))
    return

if __name__ == '__main__':
    path_to_xml = argv[1]
    file_path_to_xml = Path(path_to_xml)
    encoding_file(file_path_to_xml)
    ways_to_folder(path_to_xml)
    log_settings(file_path_to_xml.parent, ways_to_folder(path_to_xml))
    xml_to_csv(file_path_to_xml, encoding_file(file_path_to_xml))
