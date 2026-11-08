{{- define "opens3.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "opens3.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "opens3.labels" -}}
app.kubernetes.io/name: {{ include "opens3.name" . }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version | replace "+" "_" }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: opens3
{{- end -}}

{{- define "opens3.clerkHeadless" -}}
{{ include "opens3.fullname" . }}-clerk-hl
{{- end -}}

{{- define "opens3.clerkService" -}}
{{ include "opens3.fullname" . }}-clerk
{{- end -}}

{{- define "opens3.volumeHeadless" -}}
{{ include "opens3.fullname" . }}-volume-hl
{{- end -}}

{{- define "opens3.s3Service" -}}
{{ include "opens3.fullname" . }}-s3
{{- end -}}

{{- define "opens3.filerService" -}}
{{ include "opens3.fullname" . }}-filer
{{- end -}}

{{- define "opens3.filerUrl" -}}
{{- /* Unified filer serves S3 + path API on one port. */ -}}
http://{{ include "opens3.filerService" . }}.{{ .Release.Namespace }}.svc.cluster.local:{{ int (.Values.filer.port | default 8333) }}
{{- end -}}

{{- define "opens3.postgresService" -}}
{{ include "opens3.fullname" . }}-postgres
{{- end -}}

{{- define "opens3.postgresSecret" -}}
{{ include "opens3.fullname" . }}-postgres
{{- end -}}

{{/*
Filer DATABASE_URL: explicit filer.databaseUrl, else bundled postgres service.
*/}}
{{- define "opens3.filerDatabaseUrl" -}}
{{- if .Values.filer.databaseUrl -}}
{{- .Values.filer.databaseUrl -}}
{{- else if and (eq (.Values.filer.metaBackend | default "") "postgres") (.Values.filer.postgres.enabled | default false) -}}
{{- $user := .Values.filer.postgres.user | default "opens3" -}}
{{- $pass := .Values.filer.postgres.password | default "opens3" -}}
{{- $db := .Values.filer.postgres.database | default "opens3" -}}
postgres://{{ $user }}:{{ $pass }}@{{ include "opens3.postgresService" . }}.{{ .Release.Namespace }}.svc.cluster.local:5432/{{ $db }}
{{- end -}}
{{- end -}}

{{- define "opens3.clerkPeers" -}}
{{- $fullname := include "opens3.fullname" . -}}
{{- $ns := .Release.Namespace -}}
{{- $hl := include "opens3.clerkHeadless" . -}}
{{- $port := int .Values.clerk.port -}}
{{- $replicas := int .Values.clerk.replicas -}}
{{- /* Single replica: empty peers → immediate single-node Raft leader (no self-peer election). */ -}}
{{- if le $replicas 1 -}}
{{- else -}}
{{- $peers := list -}}
{{- range $i := until $replicas -}}
{{- $peers = append $peers (printf "http://%s-clerk-%d.%s.%s.svc.cluster.local:%d" $fullname $i $hl $ns $port) -}}
{{- end -}}
{{- join "," $peers -}}
{{- end -}}
{{- end -}}

{{- define "opens3.clerkUrl" -}}
http://{{ include "opens3.clerkService" . }}.{{ .Release.Namespace }}.svc.cluster.local:{{ int .Values.clerk.port }}
{{- end -}}

{{/*
Persistence type: hostPath (default) | pvc
Pass dict: root=. component=.Values.clerk.persistence (optional override type)
*/}}
{{- define "opens3.persistType" -}}
{{- $root := .root -}}
{{- $comp := .component | default dict -}}
{{- $override := $comp.type | default "" -}}
{{- if $override -}}
{{- $override -}}
{{- else -}}
{{- $root.Values.persistence.type | default "hostPath" -}}
{{- end -}}
{{- end -}}

{{/*
Host path root for a component (clerk|volume|postgres).
Pass dict: root=. component="clerk"
*/}}
{{- define "opens3.hostPath" -}}
{{- $root := .root -}}
{{- $comp := .component -}}
{{- $base := (($root.Values.persistence.hostPath).root) | default "/var/lib/opens3" -}}
{{- printf "%s/%s/%s/%s" $base $root.Release.Namespace (include "opens3.fullname" $root) $comp -}}
{{- end -}}
