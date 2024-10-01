import io
from datetime import datetime
from datetime import timedelta


def _durationSecondsFromNow(timeStr, format: str) -> bool:
    now = datetime.now()
    try:
        date = datetime.strptime(timeStr, format)
        duration = date - now
        return duration.total_seconds()
    except Exception as e:
        print("error on _durationSecondsFromNow ::: ", e)
        return None


if __name__ == "__main__":
    until_from_now = _durationSecondsFromNow("20240121T230000Z", "%Y%m%dT%H%M%SZ")
    if until_from_now == None or until_from_now < 0:
        print("Expired --- ", until_from_now)
    else:
        print("still working")
