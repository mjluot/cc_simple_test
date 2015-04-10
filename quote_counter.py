import re
from collections import Counter
from mrcc import CCJob

q_regexp = re.compile('"[^"\n]+"[^"\n]{1,10}nietzsche')

def get_quote_count(data, ctr=None):
  if ctr is None:
    ctr = Counter()
  quotes = q_regexp.findall(data.lower())
  
  filt_q = []
  for q in quotes:
    if len(q) < 150:
      filt_q.append(q)
  ctr.update(quotes)

  return ctr

class QuoteCounter(CCJob):
  def process_record(self, record):

    if record['Content-Type'] != 'text/plain':
      return
    data = record.payload.read()
    for quote, count in get_quote_count(data).items():
      yield quote, 1
    self.increment_counter('commoncrawl', 'processed_pages', 1)
if __name__ == '__main__':
  QuoteCounter.run()
