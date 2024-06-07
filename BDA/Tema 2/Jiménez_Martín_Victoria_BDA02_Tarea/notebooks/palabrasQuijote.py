
from mrjob.job import MRJob
from mrjob.step import MRStep
import re

WORD_RE = re.compile(r"[\w']+")

class MostUsedWords(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words,
                   reducer=self.reducer_count_words),
            MRStep(reducer=self.reducer_find_top_words)
        ]

    def mapper_get_words(self, _, line):
        for word in WORD_RE.findall(line):
            yield word.lower(), 1

    def reducer_count_words(self, word, counts):
        yield None, (sum(counts), word)

    def reducer_find_top_words(self, _, word_count_pairs):
        top_words = sorted(word_count_pairs, reverse=True)[:10]
        for count, word in top_words:
            yield word, count

if __name__ == '__main__':
    MostUsedWords.run()
