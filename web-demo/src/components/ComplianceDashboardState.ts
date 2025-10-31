import type { FeatureNode } from './ComplianceDashboard'

export class ComplianceDashboardState {
  private searchQuery: string = ''
  private statusFilter: string | null = null
  private expandedNodes: Set<string> = new Set()
  private onStateChange?: () => void

  constructor(onStateChange?: () => void) {
    this.onStateChange = onStateChange
    // Initialize all nodes as expanded
    this.expandedNodes.add('all')
  }

  getSearchQuery(): string {
    return this.searchQuery
  }

  getStatusFilter(): string | null {
    return this.statusFilter
  }

  getExpandedNodes(): Set<string> {
    return new Set(this.expandedNodes)
  }

  setSearchQuery(query: string): void {
    this.searchQuery = query
    this.onStateChange?.()
  }

  setStatusFilter(filter: string | null): void {
    this.statusFilter = filter
    this.onStateChange?.()
  }

  toggleNodeExpansion(nodeId: string): void {
    if (this.expandedNodes.has(nodeId)) {
      this.expandedNodes.delete(nodeId)
    } else {
      this.expandedNodes.add(nodeId)
    }
    this.onStateChange?.()
  }

  isNodeExpanded(nodeId: string): boolean {
    return this.expandedNodes.has(nodeId) || this.expandedNodes.has('all')
  }

  shouldShowNode(node: FeatureNode): boolean {
    // Filter by status
    if (this.statusFilter && node.status !== this.statusFilter) {
      // Check if any children match
      if (node.children) {
        return node.children.some(child => this.shouldShowNode(child))
      }
      return false
    }

    // Filter by search query
    if (
      this.searchQuery &&
      !node.name.toLowerCase().includes(this.searchQuery.toLowerCase())
    ) {
      // Check if any children match
      if (node.children) {
        return node.children.some(child => this.shouldShowNode(child))
      }
      return false
    }

    return true
  }

  // Event handlers
  handleSearchInput(event: Event): void {
    const input = event.target as HTMLInputElement
    this.setSearchQuery(input.value)
  }

  handleStatusFilter(status: string | null): void {
    this.setStatusFilter(status)
  }

  toggleNodeState(nodeId: string): void {
    this.toggleNodeExpansion(nodeId)
  }
}
