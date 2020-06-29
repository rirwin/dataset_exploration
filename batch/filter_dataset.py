from pyspark.sql import SparkSession
biz_ids = set([
    "f9NumwFMBDn751xgFiRbNA",
    "Yzvjg0SayhoZgCljUJRF9Q",
    "XNoUzKckATkOD1hP6vghZg",
    "6OAZjbxqM5ol29BuHsil3w",
    "51M2Kk903DFYI6gnB5I6SQ",
    "cKyLV5oWZJ2NudWgqs8VZw",
    "oiAlXZPIFm2nBCt0DHLu_Q",
    "ScYkbYNkDgCneBrD9vqhCQ",
    "pQeaRpvuhoEqudo3uymHIQ",
    "EosRKXIGeSWFYWwpkbhNnA",
    "MbZMmwo-eL0Jnm_Yb9KJrA",
    "7Dv4_HAxsxvadEsT5fxQBg",
    "M_guz7Dj7hX0evS672wIwA",
    "JjJs3o60uQCfctDjs45cmA",
    "kOICO53wbOiOJcKuCgOQ3A",
    "rqcOZePlVvJP9EtzldIz0w",
    "uZuh51lXu7tsrC8RAwkg1A",
    "nIEhsGbw0vJuYl05bzzj6Q",
    "edQoeeBFUTYGwnUSE0tGPg",
    "Vwo64kNYDjKi98gUUv4-iQ",
    "CsLQLiRoafpJPJSkNX2h5Q",
    "x3Po6tJGb729u_HJPY6UCA",
    "idgGrA8gt699JDUPKyiHJw",
    "mKTq1T_IAplDpHUcMzOXkw",
    "eBEfgOPG7pvFhb2wcG9I7w",
    "lu7vtrp_bE9PnxWfA8g4Pg",
    "1wWneWD_E1pBIyVpdHMaQg",
    "0pOlmHVeidsh63iAWdTAfg",
    "07cgbTbANYhVDfzTMOkB9w",
    "007Dg4ESDVacWcC4Vq704Q",
    "9sRGfSVEfLhN_km60YruTA",
    "pcaQDBM6r0PWTXfYZK6RdA",
    "DCsS3SgVFO56F6wRO_ewgA",
    "vjTVxnsQEZ34XjYNS-XUpA",
    "03x6ZlJ7s39DHqfTJU1Ecg",
    "UqBTL1dq9QcOISikgeknow",
    "fnZrZlqW1Z8iWgTVDfv_MA",
    "kHCTmEekJJwYsJEy7xYM5w",
    "rVBPQdeayMYht4Uv_FOLHg",
    "_4Oe9V-qTpU5iemM9bphlA",
    "PslhllUwcQFavRHp-lyMOQ",
    "98hyK2QEUeI8v2y0AghfZA",
    "fhNf_sg-XzZ3e7HEVGuOZg",
    "LoRef3ChgZKbxUio-sHgQg",
    "Ga2Bt7xfqoggTypWD5VpoQ",
    "_xOeoXfPUQTNlUAhXl32ug",
    "xFc50drSPxXkcLvX5ygqrg",
    "oPfzMA_kA9NQwG-h-JkGzA",
    "jutfk7U4GV899q6qfYp5dA",
    "tLpkSwdtqqoXwU0JAGnApw",
    "Sd75ucXKoZUM2BEfBHFUOg",
    "5XMKDYmMGSKkCkrYoELxzg",
    "9JCjKd6eFXsAMVwouTh_4Q",
    "Mmd5WDFq9hHcQ3uClngGjQ",
    "lK-wuiq8b1TuU7bfbQZgsg",
    "gflx3mAzi4iG20NOUz19eA",
    "YFsb1ydMxFLrxtJ3CffVhw",
    "3BfGGIJn8lxvu1k3ZZnL1w",
    "rdg0b0tHKvSCQNl6u3tS_A",
    "LAoSegVNU4wx4GTA8reB6A",
    "tF2ovXGBW6EeaPDWOxjpbg",
    "-qjn24n8HYF6It9GQrQntw",
    "7uYJJpwORUbCirC1mz8n9Q",
    "ZkzutF0P_u0C0yTulwaHkA",
    "EjRyYGHUxlwxYY8N73vv2w",
    "EQcCkiXbZARWGhVU02BYcw",
    "bierVTEi44nJtFVQ01N-yQ",
    "y2CeHDAglfBz26nNxvwwow",
    "bevuR6hYNuXCh9W0goe2kA",
    "UyZqOcWxShRRtACCkZFkpQ",
    "1TLZtAkDMnCvTyE4wmGWYg",
    "5ZgMwaMPLV4EB71KtLg7EQ",
    "Ar7b3_R0OBrSIbRmGvhWwg",
    "0QjROMVW9ACKjhSEfHqNCQ",
    "OT-8IUWo_2M-rHddjzz_Cg",
    "umDBj-8WUNkNBODa6P0G-Q",
    "rZSS1JzizAKTIWXxU1g_hA",
    "OLciBerMDmD_WYzfYbmkqA",
    "FrMRHhzT-qUzv1-Nh1IOjQ",
    "53Q2c9qMLEjD9r1wMn6Q8g",
    "iAtCgs1XXPeHVrftHOv1dQ",
    "gsWnxCMru_x4-4izu3kiXA",
    "RrapAhd8ZxCj-iue7fu9FA",
    "7j0kor_fkeYhyEpXh4OpnQ",
    "wUG9y_Yoq7Mfxz_9OdjI8Q",
    "PRRHaB4zge1cqskFWvQ9ZQ",
    "OWkS1FXNJbozn-qPg3LWxg",
    "Bom8MY07_YloY1R2EXrpdA",
    "UiyjUa8CQmygujhe-ZYlsw",
    "j9bWpCRwpDVfwVT_V85qeA",
    "6GHwgKNlvfIMUpFaxgBjUA",
    "dee5uffullqnbI7VM0K7FA",
    "bBhbxPjxDZSYVD-3tjUwnw",
    "efdPLo00sOD-1Wf77hg5jQ",
    "Ir_QIzs-4o9ElOtiGuxJrw",
    "GtQPbazps0jt7_cJghfmsA",
    "7drzQefNQrcf8pswAXcc-A",
    "0Y5Kzo8PWHTjk0tlfAKcDQ",
    "ER7zYKFy2YZVovBpElsU4w",
    "w-ZTkkXefxTEHlgpKA55mQ",
])
spark = SparkSession.builder.appName("FilterDataset").getOrCreate()
rdd = spark.read.json('/Users/ryanirwin/Downloads/yelp_academic_dataset_tip.json').rdd \
    .map(lambda d: d) \
    .filter(lambda d: d['business_id'] in biz_ids)

df = rdd.toDF()
df.coalesce(1).write.format('json').save('tip_small')


rdd = spark.read.json('/Users/ryanirwin/Downloads/yelp_academic_dataset_checkin.json').rdd \
    .map(lambda d: d) \
    .filter(lambda d: d['business_id'] in biz_ids)

df = rdd.toDF()
df.coalesce(1).write.format('json').save('checkin_small')


rdd = spark.read.json('/Users/ryanirwin/Downloads/yelp_academic_dataset_review.json').rdd \
    .map(lambda d: d) \
    .filter(lambda d: d['business_id'] in biz_ids)

df = rdd.toDF()
df.coalesce(1).write.format('json').save('review_small')

# biz attributes is a mess
rdd = spark.read.json('/Users/ryanirwin/Downloads/yelp_academic_dataset_business.json').rdd \
    .map(lambda d: {'business_id': d['business_id'], 'name': d['name'], 'city': d['city'], 'state': d['state']}) \
    .filter(lambda d: d['business_id'] in biz_ids)

df = rdd.toDF()
df.coalesce(1).write.format('json').save('business_small')




spark.stop()
