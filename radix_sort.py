def radix_sort(data, digit, base):
  sorted_data = []
  buckets = []
  for i in range(base):
    buckets.append([])
  for num in data:
    if len(str(num)) - 1 < digit:
      buckets[0].append(num)
    else:
      buckets[int(str(num)[-1 - digit])].append(num)

  #flatten the bucketed list and return it.
  return [item for sublist in buckets for item in sublist]


