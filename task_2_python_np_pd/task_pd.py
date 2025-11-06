import pandas as pd

data = pd.read_csv("E:/tasks/task_2/data/adult.data.csv")
print(data.head())

# 1. Посчитайте, сколько мужчин и женщин (признак sex) представлено в этом датасете

print("\nРаспределение по полу:")
sex = data.pivot_table(
    index = 'sex', 
    aggfunc = 'size'
)
print(sex)
print("-" * 90)

# 2. Каков средний возраст мужчин (признак age) по всему датасету?

mean_age_male = data.pivot_table(
    values = 'age', 
    index = 'sex', 
    aggfunc = 'mean'
).loc['Male']

print(f"Средний возраст мужчин: {mean_age_male}")
print("-" * 90)

# 3. Какова доля граждан Соединенных Штатов (признак native-country)

us_share = data.pivot_table(
    index = 'native-country', 
    aggfunc = 'size'
)['United-States'] / len(data)

print(f"Доля людей из United-States: {us_share}")
print("-" * 90)

# 4-5. Рассчитайте среднее значение и среднеквадратичное отклонение возраста тех, кто получает более 50K 
# в год (признак salary) и тех, кто получает менее 50K в год

age_stats = data.pivot_table(
    values = 'age',
    index = 'salary',
    aggfunc = ['mean', 'std']
)
print("Статистика возраста по уровню дохода (Среднее и СКО):")
print(age_stats)
print("-" * 90)

# 6. Правда ли, что люди, которые получают больше 50k, имеют минимум высшее образование? (признак education – Bachelors, 
# Prof-school, Assoc-acdm, Assoc-voc, Masters или Doctorate)

higher_education = ['Bachelors', 'Prof-school', 'Assoc-acdm', 'Assoc-voc', 'Masters', 'Doctorate']
data['is_higher_ed'] = data['education'].isin(higher_education)

all_high_degree = (data[data['salary'] == '>50K']['is_higher_ed'] == True).all()
print(f"Все ли с доходом > 50K имеют высшее образование? {all_high_degree}")
print("-" * 90)

# 7. Выведите статистику возраста для каждой расы (признак race) и каждого пола. Используйте groupby и describe. Найдите таким 
# образом максимальный возраст мужчин расы Asian-Pac-Islander.

# Статистика возраста по расе и полу
age_stats = data.groupby(['race', 'sex'])['age'].describe()
print("Статистика возраста по расе и полу:")
print(age_stats)
print("-" * 90)

# Максимальный возраст мужчин Asian-Pac-Islander
max_age_asian_male = data[
    (data['race'] == 'Asian-Pac-Islander') &
    (data['sex'] == 'Male')
]['age'].max()

print(f"Максимальный возраст мужчин Asian-Pac-Islander: {max_age_asian_male} лет")
print("-" * 90)

# 8. Среди кого больше доля зарабатывающих много (>50K): среди женатых или холостых мужчин (признак marital-status)? Женатыми считаем тех,
# у кого marital-status начинается с Married (Married-civ-spouse, Married-spouse-absent или Married-AF-spouse), остальных считаем холостыми.

data['is_married'] = data['marital-status'].str.startswith('Married')

men = data[data['sex'] == 'Male']

result = men.groupby('is_married')['salary'].apply(
    lambda x: round((x == '>50K').mean() * 100, 2)
).reset_index()

result['status'] = result['is_married'].map({True: 'Женат', False: 'Холост'})

print("Процент мужчин с зарплатой > 50K:")
print(result[['status', 'salary']].to_string(index = False, header = ['Статус', 'Процент']))

if result.loc[0, 'salary'] > result.loc[1, 'salary']:
    print("\nВывод: Среди женатых мужчин доля высокооплачиваемых выше")
else:
    print("\nВывод: Среди холостых мужчин доля высокооплачиваемых выше")
print("-" * 90)

# 9. Какое максимальное число часов человек работает в неделю (признак hours-per-week)? Сколько людей 
# работают такое количество часов и каков 

max_hours = data['hours-per-week'].max()
max_workers = data[data['hours-per-week'] == max_hours]
high_earners_pct = (max_workers['salary'] == '>50K').mean() * 100

print(f"Максимальное количество рабочих часов в неделю: {max_hours} ч")
print(f"Людей с таким графиком: {len(max_workers)}")
print(f"Из них высокооплачиваемых: {high_earners_pct:.1f}%")
print("-" * 90)

# 10. Посчитайте среднее время работы (hours-per-week) зарабатывающих мало и много (salary) для каждой страны (native-country).

work_hours = data.pivot_table(
    values='hours-per-week',
    index='native-country',
    columns='salary',
    aggfunc='mean'
).round(1)

work_hours.columns = ['До 50K', 'Свыше 50K']

print("Среднее количество рабочих часов в неделю:")
print(work_hours)
print("-" * 90)

# 11.Сгруппируйте людей по возрастным группам young, adult, retiree, где:
#   young соответствует 16-35 лет
#   adult - 35-70 лет
#   retiree - 70-100 лет
# Проставьте название соответсвтуещей группы для каждого человека в новой колонке AgeGroup

bins = [0, 16, 35, 70, 100]  
labels = ['<16', 'young', 'adult', 'retiree']

data['AgeGroup'] = pd.cut(
    data['age'],
    bins = bins,
    labels = labels,
    right = False 
)

print("Группы по возрастам: ")
print(data[['age', 'AgeGroup']].head(10))
print("-" * 90)

# 12-13. Определите количество зарабатывающих >50K в каждой из возрастных групп (колонка AgeGroup), а также выведите
# название возрастной группы, в которой чаще зарабатывают больше 50К (>50K)

high_earners_by_age = data[data['salary'] == '>50K'].groupby('AgeGroup', observed = True).size()
print("Количество людей с доходом > 50K по возрастным группам:")
print(high_earners_by_age)
print("-" * 90)

total_by_age = data.groupby('AgeGroup', observed = True).size()
percentage_high_earners = (high_earners_by_age / total_by_age * 100).round(2)

print("\nДоля высокооплачиваемых по возрастным группам (%):")
print(percentage_high_earners)

max_group = percentage_high_earners.idxmax()
max_percentage = percentage_high_earners.max()

print(f"\nВывод: Наибольшая доля высокооплачиваемых ({max_percentage}%) в группе '{max_group}'")
print("-" * 90)

# 14. Сгруппируйте людей по типу занятости (колонка occupation) и определите количество людей в каждой группе. 
# После чего напишите функциюю фильтрации filter_func, которая будет возвращать только те группы, в которых средний возраст
# (колонка age) не больше 40 и в которых все работники отрабатывают более 5 часов в неделю (колонка hours-per-week)

occupation_stats = data.groupby('occupation').agg(
    mean_age = ('age', 'mean'),               # средний возраст
    min_hours = ('hours-per-week', 'min'),    # минимальное количество часов
    count = ('age', 'size')                   # количество человек
).reset_index()


def filter_func(group_stats):

    return (group_stats['mean_age'] <= 40) & (group_stats['min_hours'] > 5)


filtered_occupations = occupation_stats[occupation_stats.apply(filter_func, axis=1)]

# Результат
print("Группы, соответствующие условиям:")
print(f"Всего групп: {len(filtered_occupations)}")
filtered_occupations