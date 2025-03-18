"""
플러그인 시스템 구현
"""
import importlib
import inspect
import pkgutil
from typing import Any, Dict, List, Optional, Set, Type, TypeVar

from dteg.extractors.base import Extractor
from dteg.loaders.base import Loader

T = TypeVar("T")


class PluginRegistry:
    """플러그인 등록 및 관리"""

    _extractor_registry: Dict[str, Type[Extractor]] = {}
    _loader_registry: Dict[str, Type[Loader]] = {}

    @classmethod
    def register_extractor(cls, name: str, extractor_class: Type[Extractor]) -> None:
        """Extractor 클래스 등록

        Args:
            name: Extractor 유형 이름
            extractor_class: Extractor 구현 클래스
        """
        cls._extractor_registry[name] = extractor_class

    @classmethod
    def register_loader(cls, name: str, loader_class: Type[Loader]) -> None:
        """Loader 클래스 등록

        Args:
            name: Loader 유형 이름
            loader_class: Loader 구현 클래스
        """
        cls._loader_registry[name] = loader_class

    @classmethod
    def get_extractor(cls, name: str) -> Type[Extractor]:
        """이름으로 Extractor 클래스 검색

        Args:
            name: Extractor 유형 이름

        Returns:
            Extractor 구현 클래스

        Raises:
            ValueError: 등록되지 않은 Extractor 유형
        """
        if name not in cls._extractor_registry:
            raise ValueError(f"등록되지 않은 Extractor 유형: {name}")
        return cls._extractor_registry[name]

    @classmethod
    def get_loader(cls, name: str) -> Type[Loader]:
        """이름으로 Loader 클래스 검색

        Args:
            name: Loader 유형 이름

        Returns:
            Loader 구현 클래스

        Raises:
            ValueError: 등록되지 않은 Loader 유형
        """
        if name not in cls._loader_registry:
            raise ValueError(f"등록되지 않은 Loader 유형: {name}")
        return cls._loader_registry[name]

    @classmethod
    def list_extractors(cls) -> List[str]:
        """등록된 모든 Extractor 유형 나열"""
        return list(cls._extractor_registry.keys())

    @classmethod
    def list_loaders(cls) -> List[str]:
        """등록된 모든 Loader 유형 나열"""
        return list(cls._loader_registry.keys())


def _is_subclass_of(cls: Any, base_class: Type[T]) -> bool:
    """클래스가 기본 클래스의 하위 클래스인지 확인"""
    return (
        inspect.isclass(cls)
        and issubclass(cls, base_class)
        and cls is not base_class
    )


def discover_plugins(package_names: Optional[List[str]] = None) -> None:
    """지정된 패키지에서 플러그인 검색 및 등록

    Args:
        package_names: 검색할 패키지 이름 목록 (기본값: ["dteg.extractors", "dteg.loaders"])
    """
    if package_names is None:
        package_names = ["dteg.extractors", "dteg.loaders"]

    for package_name in package_names:
        package = importlib.import_module(package_name)

        # 패키지 내 모든 모듈 순회
        for _, module_name, _ in pkgutil.walk_packages(package.__path__, f"{package_name}."):
            try:
                module = importlib.import_module(module_name)
            except ImportError:
                continue

            # 모듈 내 모든 클래스 순회
            for name, cls in inspect.getmembers(module):
                # Extractor 구현체 등록
                if _is_subclass_of(cls, Extractor) and hasattr(cls, "TYPE"):
                    PluginRegistry.register_extractor(cls.TYPE, cls)

                # Loader 구현체 등록
                if _is_subclass_of(cls, Loader) and hasattr(cls, "TYPE"):
                    PluginRegistry.register_loader(cls.TYPE, cls)


def create_extractor(extractor_type: str, config: Dict[str, Any]) -> Extractor:
    """Extractor 인스턴스 생성

    Args:
        extractor_type: Extractor 유형
        config: 설정 딕셔너리

    Returns:
        Extractor 인스턴스
    """
    extractor_class = PluginRegistry.get_extractor(extractor_type)
    return extractor_class(config)


def create_loader(loader_type: str, config: Dict[str, Any]) -> Loader:
    """Loader 인스턴스 생성

    Args:
        loader_type: Loader 유형
        config: 설정 딕셔너리

    Returns:
        Loader 인스턴스
    """
    loader_class = PluginRegistry.get_loader(loader_type)
    return loader_class(config) 