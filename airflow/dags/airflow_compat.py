"""
Airflow Compatibility & Testing Harness.

Provides standard Airflow DAG and Operator representations for environments
where native POSIX-dependent Airflow cannot run (e.g., native Windows without WSL/Docker).

Preserves exact Airflow API signatures, task dependency bitshifts (>> / <<),
context managers (with DAG(...) as dag:), and execution lifecycles.
"""

from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Set, Union


class BaseOperator:
    """Base class emulating Airflow BaseOperator."""

    def __init__(
        self,
        task_id: str,
        retries: int = 0,
        retry_delay: Optional[timedelta] = None,
        execution_timeout: Optional[timedelta] = None,
        dag: Optional["DAG"] = None,
        **kwargs: Any,
    ):
        self.task_id = task_id
        self.retries = retries
        self.retry_delay = retry_delay or timedelta(seconds=0)
        self.execution_timeout = execution_timeout
        self.upstream_list: Set["BaseOperator"] = set()
        self.downstream_list: Set["BaseOperator"] = set()
        self.kwargs = kwargs

        self.dag = dag or DAGContext.current_dag()
        if self.dag:
            self.dag.add_task(self)

    def set_downstream(self, other: Union["BaseOperator", List["BaseOperator"]]) -> Union["BaseOperator", List["BaseOperator"]]:
        """Sets downstream dependency."""
        targets = [other] if isinstance(other, BaseOperator) else other
        for target in targets:
            self.downstream_list.add(target)
            target.upstream_list.add(self)
        return other

    def set_upstream(self, other: Union["BaseOperator", List["BaseOperator"]]) -> Union["BaseOperator", List["BaseOperator"]]:
        """Sets upstream dependency."""
        targets = [other] if isinstance(other, BaseOperator) else other
        for target in targets:
            self.upstream_list.add(target)
            target.downstream_list.add(self)
        return other

    def __rshift__(self, other: Union["BaseOperator", List["BaseOperator"]]) -> Union["BaseOperator", List["BaseOperator"]]:
        return self.set_downstream(other)

    def __lshift__(self, other: Union["BaseOperator", List["BaseOperator"]]) -> Union["BaseOperator", List["BaseOperator"]]:
        return self.set_upstream(other)

    def execute(self, context: Optional[Dict[str, Any]] = None) -> Any:
        """Executes the task."""
        return None

    def __repr__(self) -> str:
        return f"<Task({self.task_id})>"


class PythonOperator(BaseOperator):
    """Emulates Airflow PythonOperator."""

    def __init__(
        self,
        task_id: str,
        python_callable: Callable,
        op_kwargs: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ):
        super().__init__(task_id=task_id, **kwargs)
        self.python_callable = python_callable
        self.op_kwargs = op_kwargs or {}

    def execute(self, context: Optional[Dict[str, Any]] = None) -> Any:
        """Executes the python_callable with op_kwargs."""
        return self.python_callable(**self.op_kwargs)


class EmptyOperator(BaseOperator):
    """Emulates Airflow EmptyOperator (or DummyOperator)."""

    def execute(self, context: Optional[Dict[str, Any]] = None) -> None:
        return None


# Backward compatibility alias
DummyOperator = EmptyOperator


class DAGContext:
    """Thread-local stack for DAG context manager tracking."""
    _current_dag: Optional["DAG"] = None

    @classmethod
    def set_current_dag(cls, dag: Optional["DAG"]) -> None:
        cls._current_dag = dag

    @classmethod
    def current_dag(cls) -> Optional["DAG"]:
        return cls._current_dag


class DAG:
    """Emulates Airflow DAG container."""

    def __init__(
        self,
        dag_id: str,
        description: str = "",
        schedule_interval: Optional[str] = "@daily",
        start_date: Optional[datetime] = None,
        catchup: bool = False,
        default_args: Optional[Dict[str, Any]] = None,
        max_active_runs: int = 1,
        tags: Optional[List[str]] = None,
        **kwargs: Any,
    ):
        self.dag_id = dag_id
        self.description = description
        self.schedule_interval = schedule_interval
        self.start_date = start_date
        self.catchup = catchup
        self.default_args = default_args or {}
        self.max_active_runs = max_active_runs
        self.tags = tags or []
        self.kwargs = kwargs

        self.tasks: List[BaseOperator] = []
        self.task_dict: Dict[str, BaseOperator] = {}

    def add_task(self, task: BaseOperator) -> None:
        """Registers a task in the DAG."""
        if task.task_id not in self.task_dict:
            # Apply default_args if not explicitly set on task
            if self.default_args:
                if task.retries == 0 and "retries" in self.default_args:
                    task.retries = self.default_args["retries"]
                if task.retry_delay == timedelta(seconds=0) and "retry_delay" in self.default_args:
                    task.retry_delay = self.default_args["retry_delay"]
                if task.execution_timeout is None and "execution_timeout" in self.default_args:
                    task.execution_timeout = self.default_args["execution_timeout"]

            self.tasks.append(task)
            self.task_dict[task.task_id] = task

    def get_task(self, task_id: str) -> BaseOperator:
        """Retrieves task by ID."""
        if task_id not in self.task_dict:
            raise KeyError(f"Task '{task_id}' not found in DAG '{self.dag_id}'")
        return self.task_dict[task_id]

    def __enter__(self) -> "DAG":
        DAGContext.set_current_dag(self)
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        DAGContext.set_current_dag(None)

    def __repr__(self) -> str:
        return f"<DAG({self.dag_id}) with {len(self.tasks)} tasks>"
