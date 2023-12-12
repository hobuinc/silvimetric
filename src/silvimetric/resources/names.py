# coding: utf-8

# generate docker-style names based on forestry verbs and forestry researchers
import random

left = [
    'bucking',
    'cutting',
    'skidding',
    'hauling',
    'loading',
    'felling',
    'limbing',
    'scattering',
    'reaping',
    'seeding',
    'drilling',
    'potting',
    'climbing',
    'wedging',
    'delimbing',
    'debarking',
    'branching',
    'trimming',
    'brimming',
    'bushing',
    'lopping'

]

# some from https://en.wikipedia.org/wiki/Category:Forestry_researchers
right = [
    'Bitterlich',
    'Clapp',
    'Covington',
    'Deam',
    'Dracea',
    'Egler',
    'Frey',
    'Fritz',
    'Gerry',
    'Gisborne',
    'Hemery',
    'Hepting',
    'Jardine',
    'Johnson',
    'Korstian',
    'Kuechler',
    'Kurz',
    'Lowman',
    'Lugo',
    'McArdle',
    'McGuire'
    'McGaughey',
    'Mendelsohn',
    'Moser',
    'Munger',
    'Muys',
    'Pinchot',
    'Schumacher',
    'Sillet',
    'Smits',
    'Strom',
    'Sudworth',
    'Ugrenovic',
    'Waugh',
    'Winslow',
    'Woolsey',
    'Zon'
  
]


def get_random_name(sep='_'):
    rng = random.SystemRandom()
    while 1:
        l = rng.choice(left).lower()
        r = rng.choice(right).lower()
        name = f'{l}-{r}'
        return name

if __name__ == '__main__':
    print(get_random_name())
