"""Dependency resolution system for intelligent table insertion ordering."""

import logging
from typing import Dict, List, Set, Tuple, Optional
from collections import defaultdict, deque
from dataclasses import dataclass

from .models import DatabaseSchema, TableInfo

logger = logging.getLogger(__name__)


@dataclass
class TableDependency:
    """Represents a table dependency relationship."""
    table: str
    depends_on: str
    foreign_key_columns: List[str]
    referenced_columns: List[str]


@dataclass
class InsertionPlan:
    """Plan for inserting data into tables in dependency order."""
    insertion_order: List[str]
    dependency_graph: Dict[str, List[str]]
    circular_dependencies: List[List[str]]
    independent_tables: List[str]
    
    def get_insertion_batches(self) -> List[List[str]]:
        """Group tables into batches that can be inserted in parallel."""
        batches = []
        remaining_tables = set(self.insertion_order)
        
        while remaining_tables:
            batch = []
            for table in list(remaining_tables):
                # Check if all dependencies are already processed
                deps = set(self.dependency_graph.get(table, []))
                if deps.issubset(set().union(*batches) if batches else set()):
                    batch.append(table)
            
            if not batch:
                # Handle circular dependencies - take one table from each cycle
                batch = [self.insertion_order[0]] if remaining_tables else []
            
            for table in batch:
                remaining_tables.discard(table)
            
            if batch:
                batches.append(batch)
        
        return batches


class DependencyResolver:
    """Resolves table dependencies and creates optimal insertion order."""
    
    def __init__(self, schema: DatabaseSchema):
        self.schema = schema
        self.dependencies: Dict[str, List[TableDependency]] = defaultdict(list)
        self.reverse_dependencies: Dict[str, List[str]] = defaultdict(list)
        self._build_dependency_graph()
    
    def _build_dependency_graph(self):
        """Build the complete dependency graph from schema."""
        for table in self.schema.tables:
            for fk in table.foreign_keys:
                if fk.referenced_table and fk.referenced_table != table.name:
                    dependency = TableDependency(
                        table=table.name,
                        depends_on=fk.referenced_table,
                        foreign_key_columns=fk.columns,
                        referenced_columns=fk.referenced_columns or ['id']
                    )
                    self.dependencies[table.name].append(dependency)
                    self.reverse_dependencies[fk.referenced_table].append(table.name)
    
    def get_dependency_graph(self) -> Dict[str, List[str]]:
        """Get simplified dependency graph (table -> [dependencies])."""
        graph = {}
        for table in self.schema.tables:
            graph[table.name] = [
                dep.depends_on for dep in self.dependencies[table.name]
            ]
        return graph
    
    def detect_circular_dependencies(self) -> List[List[str]]:
        """Detect circular dependencies using DFS."""
        visited = set()
        rec_stack = set()
        cycles = []
        
        def dfs(table: str, path: List[str]) -> bool:
            if table in rec_stack:
                # Found a cycle
                cycle_start = path.index(table)
                cycles.append(path[cycle_start:])
                return True
            
            if table in visited:
                return False
            
            visited.add(table)
            rec_stack.add(table)
            path.append(table)
            
            for dep in self.dependencies[table]:
                if dfs(dep.depends_on, path.copy()):
                    return True
            
            rec_stack.remove(table)
            return False
        
        for table in self.schema.tables:
            if table.name not in visited:
                dfs(table.name, [])
        
        return cycles
    
    def topological_sort(self) -> List[str]:
        """Perform topological sort to get insertion order."""
        # Kahn's algorithm for topological sorting
        in_degree = defaultdict(int)
        graph = defaultdict(list)
        
        # Build adjacency list and calculate in-degrees
        all_tables = {table.name for table in self.schema.tables}
        
        for table in self.schema.tables:
            if table.name not in in_degree:
                in_degree[table.name] = 0
            
            for dep in self.dependencies[table.name]:
                if dep.depends_on in all_tables:
                    graph[dep.depends_on].append(table.name)
                    in_degree[table.name] += 1
        
        # Initialize queue with tables having no dependencies
        queue = deque([table for table in all_tables if in_degree[table] == 0])
        result = []
        
        while queue:
            table = queue.popleft()
            result.append(table)
            
            # Reduce in-degree for dependent tables
            for dependent in graph[table]:
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)
        
        # Check for circular dependencies
        if len(result) != len(all_tables):
            remaining = all_tables - set(result)
            logger.warning(f"Circular dependencies detected in tables: {remaining}")
            # Add remaining tables (they have circular dependencies)
            result.extend(remaining)
        
        return result
    
    def create_insertion_plan(self) -> InsertionPlan:
        """Create a complete insertion plan."""
        dependency_graph = self.get_dependency_graph()
        circular_deps = self.detect_circular_dependencies()
        insertion_order = self.topological_sort()
        
        # Identify independent tables (no dependencies)
        independent_tables = [
            table for table, deps in dependency_graph.items() 
            if not deps
        ]
        
        return InsertionPlan(
            insertion_order=insertion_order,
            dependency_graph=dependency_graph,
            circular_dependencies=circular_deps,
            independent_tables=independent_tables
        )
    
    def get_table_dependencies(self, table_name: str) -> List[TableDependency]:
        """Get all dependencies for a specific table."""
        return self.dependencies[table_name]
    
    def get_dependent_tables(self, table_name: str) -> List[str]:
        """Get tables that depend on the given table."""
        return self.reverse_dependencies[table_name]
    
    def suggest_fk_value_sources(self, table_name: str) -> Dict[str, Dict[str, any]]:
        """Suggest FK value sources for a table based on dependencies."""
        suggestions = {}
        
        for dep in self.dependencies[table_name]:
            for i, fk_column in enumerate(dep.foreign_key_columns):
                ref_column = dep.referenced_columns[i] if i < len(dep.referenced_columns) else 'id'
                
                suggestions[fk_column] = {
                    'source_table': dep.depends_on,
                    'source_column': ref_column,
                    'strategy': 'existing_values',  # Use existing values from the table
                    'fallback_strategy': 'generate_if_empty'
                }
        
        return suggestions


def print_insertion_plan(plan: InsertionPlan, title: str = "Database Insertion Plan"):
    """Pretty print an insertion plan."""
    print(f"🎯 {title.upper()}")
    print("=" * 70)
    print()
    
    print("📊 INSERTION ORDER:")
    batches = plan.get_insertion_batches()
    for i, batch in enumerate(batches, 1):
        if len(batch) == 1:
            print(f"   {i:2d}. {batch[0]}")
        else:
            print(f"   {i:2d}. Parallel: {', '.join(batch)}")
    
    print()
    print("🔗 DEPENDENCY SUMMARY:")
    for table, deps in sorted(plan.dependency_graph.items()):
        if deps:
            print(f"   {table:<25} -> {', '.join(deps)}")
    
    if plan.circular_dependencies:
        print()
        print("⚠️  CIRCULAR DEPENDENCIES DETECTED:")
        for i, cycle in enumerate(plan.circular_dependencies, 1):
            cycle_str = " -> ".join(cycle + [cycle[0]])
            print(f"   {i}. {cycle_str}")
    
    if plan.independent_tables:
        print()
        print("🆓 INDEPENDENT TABLES (no dependencies):")
        print(f"   {', '.join(plan.independent_tables)}")
