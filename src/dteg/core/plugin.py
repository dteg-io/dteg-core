"""
플러그인 시스템 구현
"""
import importlib
import inspect
import pkgutil
from typing import Any, Dict, List, Optional, Set, Type, TypeVar

from dteg.extractors.base import Extractor
from dteg.loaders.base import Loader
from dteg.transformers.base import BaseTransformer

T = TypeVar("T")


class PluginRegistry:
    """플러그인 등록 및 관리"""

    _extractor_registry: Dict[str, Type[Extractor]] = {}
    _loader_registry: Dict[str, Type[Loader]] = {}
    _transformer_registry: Dict[str, Type[BaseTransformer]] = {}

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
    def register_transformer(cls, name: str, transformer_class: Type[BaseTransformer]) -> None:
        """Transformer 클래스 등록

        Args:
            name: Transformer 유형 이름
            transformer_class: Transformer 구현 클래스
        """
        cls._transformer_registry[name] = transformer_class

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
    def get_transformer(cls, name: str) -> Type[BaseTransformer]:
        """이름으로 Transformer 클래스 검색

        Args:
            name: Transformer 유형 이름

        Returns:
            Transformer 구현 클래스

        Raises:
            ValueError: 해당 이름의 Transformer를 찾을 수 없음
        """
        if name not in cls._transformer_registry:
            # 대소문자 구분 없이 찾기
            name_lower = name.lower()
            for key in cls._transformer_registry:
                if key.lower() == name_lower:
                    return cls._transformer_registry[key]
            
            available = ", ".join(cls._transformer_registry.keys())
            raise ValueError(
                f"Transformer '{name}' 유형을 찾을 수 없습니다. 사용 가능한 유형: {available}"
            )
        return cls._transformer_registry[name]

    @classmethod
    def list_extractors(cls) -> List[str]:
        """등록된 모든 Extractor 유형 나열"""
        return list(cls._extractor_registry.keys())

    @classmethod
    def list_loaders(cls) -> List[str]:
        """등록된 모든 Loader 유형 나열"""
        return list(cls._loader_registry.keys())

    @classmethod
    def list_transformers(cls) -> List[str]:
        """등록된 모든 Transformer 유형 목록 반환"""
        return list(cls._transformer_registry.keys())


def _is_subclass_of(cls: Any, base_class: Type[T]) -> bool:
    """클래스가 기본 클래스의 하위 클래스인지 확인"""
    return (
        inspect.isclass(cls)
        and issubclass(cls, base_class)
        and cls is not base_class
    )


def discover_plugins(package_names: Optional[List[str]] = None) -> None:
    """플러그인 발견 및 등록
    
    Args:
        package_names: 검색할 패키지 이름 목록, None이면 기본 패키지 검색
    """
    if package_names is None:
        package_names = ["dteg.extractors", "dteg.loaders", "dteg.transformers"]
    
    for package_name in package_names:
        try:
            package = importlib.import_module(package_name)
        except ImportError:
            # 패키지가 없으면 건너뜀
            continue
        
        # 패키지 내 모듈 검색
        for _, module_name, _ in pkgutil.iter_modules(package.__path__, package.__name__ + "."):
            try:
                module = importlib.import_module(module_name)
            except ImportError:
                # 모듈 로드 실패 시 건너뜀
                continue
            
            # 모듈 내 클래스 검색
            for _, obj in inspect.getmembers(module, inspect.isclass):
                # 같은 모듈에 정의된 클래스만 처리 (상속 클래스 제외)
                if obj.__module__ != module.__name__:
                    continue
                
                # Extractor 서브클래스 등록
                if _is_subclass_of(obj, Extractor) and obj is not Extractor:
                    name = obj.__name__.lower()
                    if name.endswith("extractor"):
                        name = name[:-9]  # "extractor" 접미사 제거
                    PluginRegistry.register_extractor(name, obj)
                
                # Loader 서브클래스 등록
                if _is_subclass_of(obj, Loader) and obj is not Loader:
                    name = obj.__name__.lower()
                    if name.endswith("loader"):
                        name = name[:-6]  # "loader" 접미사 제거
                    PluginRegistry.register_loader(name, obj)
                    
                # Transformer 서브클래스 등록
                if _is_subclass_of(obj, BaseTransformer) and obj is not BaseTransformer:
                    name = obj.__name__.lower()
                    if name.endswith("transformer"):
                        name = name[:-11]  # "transformer" 접미사 제거
                    PluginRegistry.register_transformer(name, obj)


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


def create_transformer(transformer_type: str, config: Dict[str, Any]) -> BaseTransformer:
    """지정된 유형과 설정으로 Transformer 인스턴스 생성
    
    Args:
        transformer_type: Transformer 유형 이름
        config: Transformer 설정
        
    Returns:
        Transformer 인스턴스
    """
    # Transformer 클래스 가져오기
    transformer_class = PluginRegistry.get_transformer(transformer_type)
    
    # Transformer 인스턴스 생성 및 반환
    return transformer_class(config) 