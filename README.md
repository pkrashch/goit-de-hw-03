# goit-de-hw-03 — Apache Spark (PySpark)

Обробка даних про користувачів, покупки та продукти за допомогою PySpark.

## Структура

```
main.py          — код усіх 6 кроків
data/            — вхідні CSV (users, purchases, products)
screenshots/     — скриншоти df.show() для кожного кроку (p1_ … p6_)
```

## Запуск

```bash
pip install pyspark
python main.py
```

## Кроки

| # | Що робиться | Скриншот |
|---|-------------|----------|
| 1 | Читання трьох CSV в окремі DataFrame | `p1_load_csv.png` |
| 2 | Видалення рядків із пропущеними значеннями (`dropna`) | `p2_dropna.png` |
| 3 | Загальна сума покупок (`quantity * price`) за категоріями | `p3_total_by_category.png` |
| 4 | Сума покупок за категоріями для віку 18–25 включно | `p4_total_by_category_18_25.png` |
| 5 | Частка (%) кожної категорії від усіх витрат групи 18–25 | `p5_share_18_25.png` |
| 6 | Топ-3 категорії з найвищим відсотком витрат (18–25) | `p6_top3_categories_18_25.png` |

Відсотки округлено до двох знаків після коми.

## Результат (крок 6)

| category | total_amount_18_25 | percentage |
|---|---|---|
| Home | 361.1 | 29.90 |
| Sports | 310.5 | 25.71 |
| Electronics | 249.6 | 20.67 |
