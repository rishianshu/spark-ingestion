from .base import ReconCheck, CheckRegistry, registry
from .builtin import PlainCountCheck, GroupCountCheck, PrimaryKeyUniquenessCheck

registry.register(PlainCountCheck)
registry.register(GroupCountCheck)
registry.register(PrimaryKeyUniquenessCheck)

__all__ = ["ReconCheck", "CheckRegistry", "registry", "PlainCountCheck"]
