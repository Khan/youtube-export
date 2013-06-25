import urllib2
import simplejson

_id_list = None


def get_youtube_ids():
    """Retrieve all the YouTube IDs (including translations) from KA.org.
    
    This will fetch all the publicly-visible YouTube IDs on the site in all
    languages and return them as a big, flat list.
    """
    global _id_list
    if _id_list is None:
        all_videos_in = urllib2.urlopen("http://www.khanacademy.org/api/v1/videos/localized/all")
        try:
            all_videos = simplejson.load(all_videos_in)
        finally:
            all_videos_in.close()

    _id_list = []
    for v in all_videos:
        _id_list += v["youtube_ids"].values()
    return _id_list

