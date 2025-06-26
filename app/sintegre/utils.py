import datetime
import pdb

# |     days  | weekday |
# |    Monday |    0    |
# |   Tuesday |    1    |
# | Wednesday |    2    |
# |  Thursday |    3    |
# |    Friday |    4    |
# |  Saturday |    5    |
# |    Sunday |    6    |


def get_last_saturday(date):
    if date.weekday() != 5:
        if date.weekday() < 5:
            date = date - datetime.timedelta(days=(date.weekday()+2))
        else:
            date = date - datetime.timedelta(days=1)
    return date


def get_last_friday(date):
    if date.weekday() != 4:
        if date.weekday() < 4:
            date = date - datetime.timedelta(days=(date.weekday()+3))
        else:
            date = date - datetime.timedelta(days=(date.weekday()-4))
    return date


def get_last_thursday(date):
    if date.weekday() != 3:
        if date.weekday() < 3:
            date = date - datetime.timedelta(days=(date.weekday()+4))
        else:
            date = date - datetime.timedelta(days=(date.weekday()-3))
    return date


def diff_week(date1, date2):
    if date1 > date2:
        return (date1 - date2).days / 7.0
    else:
        return (date2 - date1).days / 7.0


def count_elec_week(date1, date2):
    return int(diff_week(date1, date2))


def get_last_round(date):
    return get_last_thursday(date)


def get_rev_atual(first_day_of_month, date):
    return int(diff_week(first_day_of_month, get_last_saturday(date)))


def get_peso_semanas(first_day_of_month):
    weights = []
    weights.append((first_day_of_month + datetime.timedelta(days=6)).day)
    month_pmo = (first_day_of_month + datetime.timedelta(days=6)).month
    j = 1
    while (first_day_of_month + datetime.timedelta(days=7*j)).month == month_pmo:
        weights.append(7)
        j += 1
    weights.pop(-1)
    weights.append(8 - (first_day_of_month + datetime.timedelta(days=7*j)).day)

    while len(weights) < 6:
        weights.append(0)
    return weights


class ElecData:

    # Entrar apenas com data referente ao sabado (semanas eletricas)
    def __init__(self, date):

        if isinstance(date, datetime.datetime):
            date = date.date()

        # if date.weekday() != 5:
        # 	raise Exception('Essa biblioteca (wx_opweek.py) foi validada apenas para datas de sabados!')

        self.date = date
        self.first_day_of_month = get_last_saturday(
            datetime.date(date.year, date.month, 1))
        if date > self.first_day_of_month:
            aux_date = self.date + datetime.timedelta(days=6)
            # Primeiro dia do ano eletrico
            self.first_day_of_year = get_last_saturday(
                datetime.date(aux_date.year, 1, 1))

            # Ultimo dia do ano eletrico
            self.last_day_of_year = get_last_friday(
                datetime.date(aux_date.year, 12, 31))

            # primeiro dia do mes eletrico
            self.first_day_of_month = get_last_saturday(
                datetime.date(aux_date.year, aux_date.month, 1))
        else:
            self.first_day_of_year = get_last_saturday(
                datetime.date(self.date.year, 1, 1))
            self.last_day_of_year = get_last_friday(
                datetime.date(date.year, 12, 31))

        # Revisao atual em da data passada por parametro
        self.current_revision = get_rev_atual(self.first_day_of_month, date)

        # Inicio da semana Eletrica
        self.start_of_week = self.first_day_of_month + \
            datetime.timedelta(days=7*self.current_revision)

        # Numero de semanas ate a data passada por parametro
        self.num_weeks = count_elec_week(self.first_day_of_year, get_last_saturday(
            self.date)) + 1  # adicionado 1 para incluir a semana atual

        # Numero de semanas ate o primeiro dia do mes eletrico
        self.num_weeks_first_day_of_month = count_elec_week(
            self.first_day_of_year, self.first_day_of_month) + 1

        # Numero de semanas que o ano eletrico possui
        self.num_weeks_year = count_elec_week(
            self.first_day_of_year, self.last_day_of_year + datetime.timedelta(days=1))

        # Mes eletrico da data passada por parametro
        self.month_reference = (
            self.first_day_of_month + datetime.timedelta(days=6)).month

        # Ano eletrico da data passada por parametro
        self.year_reference = self.last_day_of_year.year

    def get_peso_semanas(self):
        return get_peso_semanas(self.first_day_of_month)

    def __str__(self):
        return f"""'first_day_of_year': {self.first_day_of_year}
'last_day_of_year': {self.last_day_of_year}
'first_day_of_month': {self.first_day_of_month}
'start_of_week': {self.start_of_week}
'current_revision': {self.current_revision}
'num_weeks': {self.num_weeks}
'num_weeks_year': {self.num_weeks_year}
'month_reference': {self.month_reference}
'num_weeks_first_day_of_month': {self.num_weeks_first_day_of_month}
"""
