import requests
from dotenv import load_dotenv
import os
import json
import pandas as pd
from datetime import datetime
import openpyxl
import time
import sys
from typing import List, Dict, Optional, Any


load_dotenv()
token = os.getenv('TOKEN')

if not token:
    print("❌ Ошибка: Токен не найден в .env файле!")
    print("Создайте файл .env и добавьте строку: TOKEN=ваш_токен")
    sys.exit(1)

URL = 'https://ows.goszakup.gov.kz/v3/graphql'
headers = {
    'Authorization': f'Bearer {token}',
    'Content-Type': 'application/json'
}

# Запрос с вложенными Plans внутри ContractUnits
CONTRACTS_WITH_PLANS_QUERY = """
query GetContracts($customerBin: String!, $finYear: Int!, $limit: Int!, $after: Int) {
  Contract(
    filter: {
      customerBin: $customerBin
      finYear: $finYear
    }
    limit: $limit
    after: $after
  ) {
    id
    contractNumberSys
    trdBuyNumberAnno
    trdBuyNameRu
    descriptionRu
    finYear
    contractSum
    signDate

    Supplier {
      nameRu
    }

    RefContractType {
      nameRu
    }

    RefContractStatus {
      nameRu
    }

    FaktTradeMethods {
      nameRu
    }

    ContractUnits {
      plnPointId
      itemPrice
      quantity
      totalSum
    }
  }
}
"""


def fetch_all_contracts(customer_bin: str, fin_year: int, limit: int = 200, max_pages: Optional[int] = None) -> List[Dict[str, Any]]:
    """Получить все договоры с ContractUnits
    
    Args:
        customer_bin: БИН заказчика
        fin_year: Финансовый год
        limit: Количество записей на страницу
        max_pages: Максимальное количество страниц (None = без ограничений)
    
    Returns:
        Список договоров
    """
    all_contracts = []
    after = None
    page = 1

    print(f"Получение договоров для БИН {customer_bin}, год {fin_year}...")

    while True:
        if max_pages and page > max_pages:
            print(f"⚠️  Достигнут лимит страниц: {max_pages}")
            break

        variables = {
            'customerBin': customer_bin,
            'finYear': fin_year,
            'limit': limit,
            'after': after
        }

        try:
            response = requests.post(
                URL,
                headers=headers,
                json={'query': CONTRACTS_WITH_PLANS_QUERY, 'variables': variables},
                timeout=30
            )

            if response.status_code != 200:
                print(f"❌ Ошибка API: {response.status_code}")
                if response.status_code == 429:
                    print("⏳ Rate limit, ожидание 60 секунд...")
                    time.sleep(60)
                    continue
                break

            data = response.json()

            if 'errors' in data:
                print(f"❌ GraphQL ошибки: {data['errors']}")
                break

            contracts = data.get('data', {}).get('Contract', [])

            if not contracts:
                break

            all_contracts.extend(contracts)
            print(f"✅ Страница {page}: получено {len(contracts)} записей (всего {len(all_contracts)})")

            after = contracts[-1]['id']
            page += 1

            # Небольшая задержка для избежания rate limiting
            time.sleep(0.5)

        except requests.exceptions.Timeout:
            print(f"⏱️  Таймаут на странице {page}, повтор через 5 секунд...")
            time.sleep(5)
            continue

        except requests.exceptions.ConnectionError:
            print(f"🔌 Ошибка соединения на странице {page}, повтор через 10 секунд...")
            time.sleep(10)
            continue

        except requests.exceptions.RequestException as e:
            print(f"❌ Ошибка запроса: {e}")
            break

        except Exception as e:
            print(f"❌ Неожиданная ошибка: {e}")
            break

    print(f"\n🎉 Всего получено: {len(all_contracts)} договоров")
    return all_contracts


def fetch_plans_by_ids(plan_ids: List[int]) -> Dict[int, Dict[str, Any]]:
    """Получить планы по списку ID с пагинацией
    
    Args:
        plan_ids: Список ID планов
        
    Returns:
        Словарь {plan_id: plan_data}
    """
    print(f"\nПолучение планов для {len(plan_ids)} позиций...")

    all_plans = []

    # Делаем батчами по 100 ID для избежания слишком больших запросов
    batch_size = 100

    for batch_idx in range(0, len(plan_ids), batch_size):
        batch = plan_ids[batch_idx:batch_idx + batch_size]
        print(f"  Обработка батча {batch_idx // batch_size + 1}/{(len(plan_ids) - 1) // batch_size + 1}...")

        # Пагинация внутри батча
        after = None
        page = 1

        while True:
            # Строим запрос с пагинацией
            variables = {
                "ids": batch,
                "limit": 200,
                "after": after
            }

            query_text = """
            query GetPlans($ids: [Int!], $limit: Int!, $after: Int) {
              Plans(
                filter: {
                  id: $ids
                }
                limit: $limit
                after: $after
              ) {
                id
                nameRu
                count
                price
                amount
                extraDescRu
              }
            }
            """

            try:
                response = requests.post(
                    URL,
                    headers=headers,
                    json={'query': query_text, 'variables': variables},
                    timeout=30
                )

                if response.status_code != 200:
                    print(f"⚠️  Ошибка API при получении планов: {response.status_code}")
                    if response.status_code == 429:
                        print("⏳ Rate limit, ожидание 60 секунд...")
                        time.sleep(60)
                        continue
                    break

                data = response.json()

                if 'errors' in data:
                    print(f"⚠️  GraphQL ошибка при получении планов: {data['errors']}")
                    break

                plans = data.get('data', {}).get('Plans', [])

                if not plans:
                    break

                all_plans.extend(plans)
                print(f"    Страница {page}: получено {len(plans)} планов")

                # Если получили меньше лимита, значит это последняя страница
                if len(plans) < 200:
                    break

                after = plans[-1]['id']
                page += 1

                # Задержка между запросами
                time.sleep(0.3)

            except requests.exceptions.Timeout:
                print(f"⏱️  Таймаут при получении планов, повтор через 5 секунд...")
                time.sleep(5)
                continue

            except requests.exceptions.RequestException as e:
                print(f"⚠️  Ошибка при получении планов: {e}")
                break

        # Задержка между батчами
        time.sleep(0.5)

    print(f"✅ Всего планов получено: {len(all_plans)}")

    # Создать словарь plan_id → plan_data
    plans_dict = {plan['id']: plan for plan in all_plans}
    return plans_dict


def transform_to_excel_format(contracts: List[Dict[str, Any]], plans_dict: Dict[int, Dict[str, Any]]) -> pd.DataFrame:
    """Преобразовать данные с плановыми суммами
    
    Args:
        contracts: Список договоров
        plans_dict: Словарь планов {plan_id: plan_data}
        
    Returns:
        DataFrame с данными для экспорта
    """
    print("\n📊 Трансформация данных...")

    rows = []
    stats = {'with_plan': 0, 'without_plan': 0}

    for i, contract in enumerate(contracts, 1):
        supplier_name = contract.get('Supplier', {}).get('nameRu', '') if contract.get('Supplier') else ''
        contract_type = contract.get('RefContractType', {}).get('nameRu', '') if contract.get('RefContractType') else ''
        contract_status = contract.get('RefContractStatus', {}).get('nameRu', '') if contract.get(
            'RefContractStatus') else ''
        procurement_method = contract.get('FaktTradeMethods', {}).get('nameRu', '') if contract.get(
            'FaktTradeMethods') else ''

        description = contract.get('descriptionRu') or contract.get('trdBuyNameRu') or ''

        # Получить плановую сумму из ContractUnits → Plans
        plan_sum = 0
        contract_units = contract.get('ContractUnits', [])

        if contract_units:
            for unit in contract_units:
                pln_point_id = unit.get('plnPointId')
                if pln_point_id and pln_point_id in plans_dict:
                    plan = plans_dict[pln_point_id]
                    plan_sum += plan.get('amount', 0) or 0

        if plan_sum > 0:
            stats['with_plan'] += 1
        else:
            stats['without_plan'] += 1

        # Вычисление экономии
        contract_sum_raw = contract.get('contractSum')
        contract_sum = float(contract_sum_raw) if contract_sum_raw and str(contract_sum_raw).strip() else 0
        savings = plan_sum - contract_sum if (plan_sum > 0 and contract_sum > 0) else ''

        row = {
            '№': i,
            'Номер договора в реестре договоров': contract.get('contractNumberSys', ''),
            'Номер закупки': contract.get('trdBuyNumberAnno', ''),
            'Описание договора': description,
            'Тип договора': contract_type,
            'Статус договора': contract_status,
            'Способ закупки': procurement_method,
            'Финансовый год': contract.get('finYear', ''),
            'Плановая сумма без ндс': plan_sum if plan_sum > 0 else '',
            'Сумма без ндс': contract_sum if contract_sum > 0 else '',
            'Сумма экономии без ндс': savings,
            'Поставщик': supplier_name,
            'Дата заключения': contract.get('signDate', '')
        }

        rows.append(row)

    print(f"\n📊 Статистика:")
    print(f"  С плановой суммой: {stats['with_plan']}")
    print(f"  Без плановой суммы: {stats['without_plan']}")

    df = pd.DataFrame(rows)
    print(f"✅ Преобразовано {len(df)} записей")
    return df


def export_to_excel(df: pd.DataFrame, filename: str) -> None:
    """Экспорт в Excel с форматированием
    
    Args:
        df: DataFrame для экспорта
        filename: Имя выходного файла
    """
    print(f"\n💾 Экспорт в {filename}...")

    from openpyxl.styles import Font, Alignment, PatternFill
    from openpyxl.utils import get_column_letter

    try:
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Договоры')

            workbook = writer.book
            worksheet = writer.sheets['Договоры']

            header_fill = PatternFill(start_color='366092', end_color='366092', fill_type='solid')
            header_font = Font(bold=True, color='FFFFFF')

            for cell in worksheet[1]:
                cell.font = header_font
                cell.fill = header_fill
                cell.alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)

            # Форматирование числовых колонок с разделителями тысяч
            for row in worksheet.iter_rows(min_row=2, max_row=worksheet.max_row):
                # Плановая сумма без ндс (колонка I)
                if row[8].value and isinstance(row[8].value, (int, float)):
                    row[8].number_format = '#,##0.00'
                # Сумма без ндс (колонка J)
                if row[9].value and isinstance(row[9].value, (int, float)):
                    row[9].number_format = '#,##0.00'
                # Сумма экономии без ндс (колонка K)
                if row[10].value and isinstance(row[10].value, (int, float)):
                    row[10].number_format = '#,##0.00'

            for column in worksheet.columns:
                max_length = 0
                column_letter = get_column_letter(column[0].column)

                for cell in column:
                    try:
                        if cell.value:
                            max_length = max(max_length, len(str(cell.value)))
                    except (AttributeError, TypeError, ValueError):
                        # Игнорируем ошибки преобразования значений
                        pass

                adjusted_width = min(max_length + 2, 60)
                worksheet.column_dimensions[column_letter].width = adjusted_width

            worksheet.freeze_panes = 'A2'

        print(f"✅ Файл сохранён: {filename}")

    except PermissionError:
        print(f"❌ Ошибка: Файл {filename} открыт в другой программе. Закройте файл и повторите попытку.")
        raise
    except Exception as e:
        print(f"❌ Ошибка при сохранении файла: {e}")
        raise


def main() -> None:
    """Главная функция"""
    # Получить параметры из .env или использовать значения по умолчанию
    CUSTOMER_BIN = os.getenv('CUSTOMER_BIN', '020240003361')
    FIN_YEAR = int(os.getenv('FIN_YEAR', '2025'))
    MAX_PAGES = int(os.getenv('MAX_PAGES', '0')) or None  # 0 = без ограничений
    OUTPUT_FILE = f'contracts_{FIN_YEAR}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'

    print("🚀 СТАРТ")
    print(f"📋 Параметры:")
    print(f"   БИН заказчика: {CUSTOMER_BIN}")
    print(f"   Финансовый год: {FIN_YEAR}")
    print(f"   Лимит страниц: {MAX_PAGES if MAX_PAGES else 'без ограничений'}")
    print()

    try:
        # 1. Получить договоры
        contracts = fetch_all_contracts(CUSTOMER_BIN, FIN_YEAR, max_pages=MAX_PAGES)

        if not contracts:
            print("⚠️  Договоры не найдены")
            return

        # Сохранить в кеш
        cache_file = f'contracts_raw_{FIN_YEAR}.json'
        with open(cache_file, 'w', encoding='utf-8') as f:
            json.dump(contracts, f, indent=2, ensure_ascii=False)
        print(f"💾 Кеш сохранен: {cache_file}")

        # 2. Собрать все plnPointId
        all_plan_ids = set()
        for contract in contracts:
            for unit in contract.get('ContractUnits', []):
                pln_id = unit.get('plnPointId')
                if pln_id:
                    all_plan_ids.add(pln_id)

        print(f"\n📋 Найдено уникальных plnPointId: {len(all_plan_ids)}")

        # 3. Получить планы
        if all_plan_ids:
            plans_dict = fetch_plans_by_ids(list(all_plan_ids))
        else:
            print("⚠️  plnPointId не найдены")
            plans_dict = {}

        # 4. Трансформация
        df = transform_to_excel_format(contracts, plans_dict)

        # 5. Экспорт
        export_to_excel(df, OUTPUT_FILE)

        print("\n" + "=" * 50)
        print("🎉 ГОТОВО!")
        print("=" * 50)
        print(f"📄 Файл: {OUTPUT_FILE}")

    except KeyboardInterrupt:
        print("\n\n⚠️  Прервано пользователем")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()