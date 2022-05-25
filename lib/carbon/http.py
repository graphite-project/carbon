import urllib3

# async http client connection pool
http = urllib3.PoolManager()


def httpRequest(url, values=None, headers=None, method='POST', timeout=5):
  try:
    result = http.request(
      method,
      url,
      fields=values,
      headers=headers,
      timeout=timeout)
  except BaseException as err:
    raise Exception("Error requesting %s: %s" % (url, err))

  if result.status != 200:
    raise Exception("Error response %d from %s" % (result.status, url))

  return result.data
