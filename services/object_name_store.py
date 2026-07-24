class ObjectNameStore:
    def __init__(self):
        self._store = {}

    def get_object_name(self, message_id: str) -> str:
        return self._store.get(message_id)

    def set_object_name(self, message_id: str, object_name: str):
        self._store[message_id] = object_name

    def resolve_audio_url(self, message_id: str) -> str:
        object_name = self.get_object_name(message_id)
        if object_name:
            return f"s3://audio-bucket/{object_name}"
        return ""
