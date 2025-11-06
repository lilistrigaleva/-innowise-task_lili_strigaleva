import numpy as np
import time

from itertools import product
from collections import Counter
from scipy.spatial.distance import cdist

# Part 1

#1. Дан случайный массив, поменять знак у элементов, значения которых между 3 и 8
arr = np.random.randint(-9, 10, 7)
print(arr)

mask = (arr > 3) & (arr < 8)
arr[mask] = -arr[mask]
print(f"result {arr}\n")

#2. Заменить максимальный элемент случайного массива на 0
max_idx = np.argmax(arr)
arr[max_idx] = 0
print(f"result {arr}\n")

#3. Построить прямое произведение массивов (все комбинации с каждым элементом). На вход подается двумерный массив
arr_1 = np.random.randint(-5, 6, 5)
arr_2 = np.random.randint(-5, 6, 5)

cartesian = np.array(list(product(arr_1, arr_2)))

print(f"arr_1: {arr_1}")
print(f"arr_2: {arr_2}")
print("Прямое произведение:\n", cartesian)

#4. Даны 2 массива A (8x3) и B (2x2). Найти строки в A, которые содержат элементы из каждой строки в B, 
# независимо от порядка элементов в B
a = np.random.randint(0, 10, size=(8, 3)) 
b = np.random.randint(0, 10, size=(2, 2))

print(f"A {a}")
print(f"B {b}") 

result = []

for row in a:
    print("Строка A:", row)
    match = True
    for b_row in b:
        print("Проверка B:", b_row)
        if not all(elem in row for elem in b_row):
            match = False
            print("Не все элементы найдены")
            break
    if match:
        print("Добавляем:", row)
        result.append(row)


result = np.array(result)
print(result)

#5. Дана 10x3 матрица, найти строки из неравных значений (например строка [2,2,3] остается, строка [3,3,3] удаляется)

matrix = np.random.randint(0, 10, size=(10, 3))
# matrix = np.array([
#     [1, 2, 3],
#     [3, 3, 3],
#     [4, 5, 4],
#     [6, 6, 6],
#     [7, 8, 9],
#     [2, 2, 3],
#     [1, 1, 2],
#     [5, 5, 5],
#     [0, 1, 0],
#     [9, 8, 7]
# ])

print("Исходная матрица:")
print(matrix)

filtered = np.array([row for row in matrix if len(set(row)) > 1])

print("\nСтроки с неравными значениями:")
print(filtered)

#6. Дан двумерный массив. Удалить те строки, которые повторяются

matrix = np.array([
    [1, 2, 3],
    [4, 5, 6],  
    [1, 2, 3],
    [7, 8, 9],
    [4, 5, 6]
])

unique_rows = np.unique(matrix, axis=0) 

print("Результат:")
print(unique_rows)

# Part 2

# 1. Подсчитать произведение ненулевых элементов на диагонали прямоугольной матрицы. 
# Например, для X = np.array([[1, 0, 1], [2, 0, 2], [3, 0, 3], [4, 4, 4]]) ответ 3.

# without numpy
X = [
    [1, 0, 1],
    [2, 0, 2],
    [3, 0, 3],
    [4, 4, 4]
]

product = 1
found = False

for i in range(min(len(X), len(X[0]))):
    val = X[i][i]
    if val != 0:
        product *= val
        found = True

print(product if found else 0)

# with numpy
X = np.array([
    [1, 0, 1],
    [2, 0, 2],
    [3, 0, 3],
    [4, 4, 4]
])

diag = np.diagonal(X)

non_zero_diag = diag[diag != 0]

product = np.prod(non_zero_diag)

print(product)

# 2. Даны два вектора x и y. Проверить, задают ли они одно и то же мультимножество.
# Например, для x = np.array([1, 2, 2, 4]), y = np.array([4, 2, 1, 2]) ответ True.

# without numpy
x = [1, 2, 2, 4]
y = [4, 2, 1, 2]

result = Counter(x) == Counter(y)
print(result)

# with numpy
x = np.array([1, 2, 2, 4])
y = np.array([4, 2, 1, 2])

result = np.array_equal(np.sort(x), np.sort(y))
print(result)

# 3. Найти максимальный элемент в векторе x среди элементов, перед которыми стоит ноль. 
# Например, для x = np.array([6, 2, 0, 3, 0, 0, 5, 7, 0]) ответ 5.

# without numpy
x = [6, 2, 0, 3, 0, 0, 5, 7, 0]

candidates = []

for i in range(1, len(x)):
    if x[i - 1] == 0:
        candidates.append(x[i])

result = max(candidates) if candidates else None

print(result)  


# with numpy
x = np.array([6, 2, 0, 3, 0, 0, 5, 7, 0])

indices = np.where(x[:-1] == 0)[0] + 1

candidates = x[indices]

result = np.max(candidates) if candidates.size > 0 else None

print(result)

# Реализовать кодирование длин серий (Run-length encoding). Для некоторого вектора x необходимо вернуть 
# кортеж из двух векторов одинаковой длины. Первый содержит числа, а второй - сколько раз их нужно повторить. 
# Например, для x = np.array([2, 2, 2, 3, 3, 3, 5]) ответ (np.array([2, 3, 5]), np.array([3, 3, 1])).

x = [2, 2, 2, 3, 3, 3, 5]

# without numpy
numbers = []
counts = []

current = x[0]
count = 1

for i in range(1, len(x)):
    if x[i] == current:
        count += 1
    else:
        numbers.append(current)
        counts.append(count)
        current = x[i]
        count = 1

numbers.append(current)
counts.append(count)

print((numbers, counts))

# with numpy
change_indices = np.where(np.diff(x) != 0)[0] + 1

values = np.split(x, change_indices)

numbers = np.array([group[0] for group in values])
counts = np.array([len(group) for group in values])

print((numbers, counts)) 

# 5. Даны две выборки объектов - X и Y. Вычислить матрицу евклидовых расстояний между объектами. 
# Сравните с функцией scipy.spatial.distance.cdist по скорости работы.

X = np.random.rand(1000, 5)
Y = np.random.rand(1000, 5)

# without numpy
start_cdist = time.time()

dist_cdist = cdist(X, Y, metric='euclidean')

end_cdist = time.time()
print(f"cdist: {end_cdist - start_cdist:.4f} секунд")

# with numpy
start_manual = time.time()

dist_manual = np.sqrt(((X[:, np.newaxis, :] - Y[np.newaxis, :, :]) ** 2).sum(axis=2))

end_manual = time.time()
print(f"\nРучной способ: {end_manual - start_manual:.4f} секунд")


# # Задача 6: CrunchieMunchies *
# Вы работаете в отделе маркетинга пищевой компании MyCrunch, которая разрабатывает новый вид вкусных,
# полезных злаков под названием CrunchieMunchies.

# Вы хотите продемонстрировать потребителям, насколько полезны ваши хлопья по сравнению с другими ведущими брендами,
# поэтому вы собрали данные о питании нескольких разных конкурентов.

# Ваша задача - использовать вычисления Numpy для анализа этих данных и доказать, что ваши СrunchieMunchies - самый 
# здоровый выбор для потребителей.

calorie_stats = np.loadtxt("E:/tasks/task_2/data/cereal.csv", delimiter=",")
print("\nОтсортированные данные калорийности:")
calorie_stats

average_calories = np.mean(calorie_stats)
print(f"\nСреднее количество калорий: {average_calories:.2f}")

difference = average_calories - 60
print(f"\nРазница со средним: {difference:.2f} калорий")

calorie_stats_sorted = np.sort(calorie_stats)
print("\nОтсортированные данные калорийности:")
print(calorie_stats_sorted)

median_calories = np.median(calorie_stats)
print(f"\nМедиана калорийности: {median_calories}")

percentiles = np.percentile(calorie_stats, range(0, 101, 10))
print("\nПроцентили (0-100% с шагом 10%):")
for p, val in zip(range(0, 101, 10), percentiles):
    print(f"{p}%: {val:.2f} калорий")

# Находим наименьший процентиль > 60
for p in range(1, 101):
    if np.percentile(calorie_stats, p) > 60:
        nth_percentile = p
        break

print(f"\nНаименьший процентиль > 60: {nth_percentile}%")

more_calories = np.mean(calorie_stats > 60) * 100
print(f"Процент хлопьев с калорийностью >60: {more_calories:.2f}%")

calorie_std = np.std(calorie_stats, ddof=1)
print(f"Стандартное отклонение: {calorie_std:.2f} калорий")

print("\nАнализ данных о калорийности хлопьев:")
print(f"1. Средняя калорийность: {average_calories:.1f} калорий")
print(f"2. Медианная калорийность: {median_calories:.1f} калорий")
print(f"3. {more_calories:.1f}% брендов содержат больше 60 калорий")
print(f"4. Стандартное отклонение: {calorie_std:.2f} калорий")

print("\nКлючевые выводы для маркетинга:")
print("- CrunchieMunchies содержат значительно меньше калорий, чем у большинства конкурентов.")
print(f"- Продукт попадает в топ-{nth_percentile}% по низкой калорийности.")
print("- Высокий разброс данных (стандартное отклонение) показывает, что CrunchieMunchies выделяются на фоне конкурентов.")