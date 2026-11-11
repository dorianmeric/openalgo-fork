import { Monitor } from 'lucide-react'
import { useEffect, useState } from 'react'
import { Badge } from '@/components/ui/badge'
import { cn } from '@/lib/utils'
import { useSessionStore } from '@/stores/sessionStore'

interface FooterProps {
  className?: string
}

export function Footer({ className }: FooterProps) {
  const [version, setVersion] = useState<string>('')
  const activeSessionCount = useSessionStore((s) => s.activeSessionCount)

  useEffect(() => {
    const fetchVersion = async () => {
      try {
        const response = await fetch('/auth/app-info')
        const data = await response.json()
        if (data.status === 'success') {
          setVersion(data.version)
        }
      } catch (_error) {}
    }

    fetchVersion()
  }, [])

  return (
    <footer className={cn('mt-auto border-t bg-muted/30', className)}>
      <div className="container mx-auto px-4 py-6">
        <div className="flex flex-col md:flex-row items-center justify-center gap-2 md:gap-4 text-sm text-muted-foreground">
          <span>Copyright 2026</span>
          <span className="hidden md:inline">|</span>
          <span className="text-center">Open Source Algo Platform for Everyone</span>
          {version && (
            <>
              <span className="hidden md:inline">|</span>
              <Badge variant="secondary" className="gap-1">
                <span className="opacity-75">v</span>
                <span>{version}</span>
              </Badge>
            </>
          )}
          {activeSessionCount > 0 && (
            <>
              <span className="hidden md:inline">|</span>
              <Badge variant="outline" className="gap-1">
                <Monitor className="h-3 w-3" />
                <span>
                  {activeSessionCount} {activeSessionCount === 1 ? 'session' : 'sessions'}
                </span>
              </Badge>
            </>
          )}
        </div>
      </div>
    </footer>
  )
}
