from typing import Callable, Dict, List

class EventManager:
    """
    A simple observer pattern system to handle events in the network.
    """

    def __init__(self):
        self._listeners: Dict[str, List[Callable]] = {}

    def on(self, event_name: str, callback: Callable):
        """Register a callback to an event."""
        self._listeners.setdefault(event_name, []).append(callback)

    def emit(self, event_name: str, data: dict):
        """Trigger an event with data."""
        for callback in self._listeners.get(event_name, []):
            callback(data)
