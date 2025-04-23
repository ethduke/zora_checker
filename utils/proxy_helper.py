import random
import logging

class ProxyHandler:
    """Handles loading, selecting, and formatting proxies."""

    def __init__(self, proxies):
        """Initializes the handler with a list of proxy strings."""
        self.proxies = proxies if proxies else []
        self.num_proxies = len(self.proxies)
        if self.num_proxies == 0:
            logging.warning("ProxyHandler initialized with an empty proxy list.")

    def _format_proxy(self, proxy_string):
        """Ensures the proxy string has the correct http:// or https:// prefix."""
        if not proxy_string:
            return None
        if not proxy_string.startswith("http://") and not proxy_string.startswith("https://"):
            return f"http://{proxy_string}"
        return proxy_string

    def get_initial_proxy(self):
        """Gets a random proxy formatted as a URL for initial use."""
        if not self.proxies:
            return None
        initial_proxy = random.choice(self.proxies)
        return self._format_proxy(initial_proxy)

    def get_new_random_proxy(self, current_proxy_url=None):
        """Gets a new random proxy URL, attempting to differ from the current one."""
        if not self.proxies:
            return None
        
        if self.num_proxies == 1:
            return self._format_proxy(self.proxies[0])

        new_proxy_string = random.choice(self.proxies)
        new_proxy_url = self._format_proxy(new_proxy_string)

        if new_proxy_url == current_proxy_url and self.num_proxies > 1:
            new_proxy_string = random.choice(self.proxies)
            new_proxy_url = self._format_proxy(new_proxy_string)
            
        return new_proxy_url

    @staticmethod
    def get_display_proxy(proxy_url):
        """Returns the proxy host/port part for safe logging (removes user:pass)."""
        if not proxy_url:
            return "None"
        try:
            if '@' in proxy_url:
                return proxy_url.split('@')[-1]
            else:
                return proxy_url.split('//')[-1]
        except Exception:
            return proxy_url 