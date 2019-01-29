import re

from pylatex import NoEscape, Package
from pylatex.base_classes import Environment, ContainerCommand


class LatexSection(Environment):
    """This class is generic and allow the creation of any section like

    .. code-block:: latex

        \begin{name}
            Some content
        \end{name}

    The name is provided to the constructor. No additional package will be added to the list of packages.
    """
    def __init__(self, name, **kwargs):
        self._latex_name = name
        super().__init__(**kwargs)


class HRef(ContainerCommand):
    """A class that represents an hyperlink to a web address."""

    _repr_attributes_mapping = {
        'marker': 'options',
        'text': 'arguments',
    }

    packages = [Package('hyperref')]

    def __init__(self, url, text=None):
        """
        Args
        ----
        url: str
            The url to use.
        text:
            The text that will be shown as a link to the url. Use the url if not set
        """

        self.url = url
        if text is None:
            text = url
        super().__init__(arguments=NoEscape(url), data=text)


def add_text(doc, t):
    """
    Generic function to add text to a pylatex document.
    Split the provided text to escape latex commands and then add to the container.
    """
    current_pos = 0
    for m in re.finditer(r'latex::(.+?)::', t):
        doc.append(t[current_pos: m.start()])
        doc.append(NoEscape(' ' + m.group(1) + ' '))
        current_pos = m.end()
    doc.append(t[current_pos:])
    return doc