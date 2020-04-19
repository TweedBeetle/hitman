import itertools
import random
import uuid

from trueskill import *
import operator as op
from functools import reduce


def ncr(n, r):
    r = min(r, n - r)
    numer = reduce(op.mul, range(n, n - r, -1), 1)
    denom = reduce(op.mul, range(1, r + 1), 1)
    pass
    return numer / denom


class Contestant:
    def __init__(self, value=None, id=None, environment=None):
        self.id = str(uuid.uuid4()) if id is None else id
        self.value = value
        self.rating = Rating() if environment is None else environment.Rating()

    def __repr__(self):
        return f'contestant {self.id} with mu {self.rating.mu}'


class CompetitionFFAWTA:

    def __init__(self, match_size=2, draw_probability=0):
        self.contestant_class = Contestant
        self.match_size = match_size
        self.draw_probability = draw_probability
        self.environment = TrueSkill(draw_probability=self.draw_probability)

        self.num_contestants = 0
        self.contestants = {}

    def add_contestant(self, contestant: Contestant):
        self.num_contestants += 1
        self.contestants[contestant.id] = contestant

    def add_contestant_by_id(self, contestant_id):
        if contestant_id in self.contestants.keys():
            return
        contestant = Contestant(value=None, id=contestant_id, environment=self.environment)
        self.add_contestant(contestant)

    def add_contestants_by_ids(self, contestant_ids):
        for contestant_id in contestant_ids:
            self.add_contestant_by_id(contestant_id)

    def get_contestant_ids(self):
        return [contestant.id for contestant in self.contestants.values()]

    def get_ideal_matchups(self):
        return itertools.combinations(self.get_contestant_ids(), self.match_size)

    def get_num_possible_matchups(self):
        return int(ncr(self.num_contestants, self.match_size))

    def get_matchups(self, num_matchups):

        matchups = []
        num_combinations = self.get_num_possible_matchups()

        ideal_matchups = list(self.get_ideal_matchups())

        for n in range(num_matchups):
            ind = n % num_combinations
            if ind == 0:
                random.shuffle(ideal_matchups)

            matchups.append(ideal_matchups[ind])

        return matchups

    def record_match(self, contestant_ids, ranks):
        teams = [(self.contestants[id].rating,) for id in contestant_ids]
        resulting_ratings = self.environment.rate(
            rating_groups=teams,
            ranks=ranks
        )

        for i, new_rating_tuple in enumerate(resulting_ratings):
            self.contestants[contestant_ids[i]].rating = new_rating_tuple[0]

    def leaderboard(self):
        return list(sorted(self.contestants.values(), key=lambda c: self.environment.expose(c.rating), reverse=True))

    def best(self):
        return self.leaderboard()[0]

    def best_id(self):
        return self.best().id


if __name__ == "__main__":
    # ids = [str(i) for i in range(1, 5)]
    ids = ['a', 'b', 'c', 'd', 'e']
    comp = CompetitionFFAWTA(match_size=3)
    comp.add_contestants_by_ids(ids)

    comp.record_match(['a', 'a', 'c'], [0, 1, 2])
    comp.record_match(['a', 'a', 'b'], [1, 0, 2])
    comp.record_match(['a', 'a', 'e'], [1, 2, 0])
    print(comp.leaderboard())
    print('best:')
    print(comp.best())

pass
