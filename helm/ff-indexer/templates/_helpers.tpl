{{/*
Expand the name of the chart.
*/}}
{{- define "ff-indexer.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "ff-indexer.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "ff-indexer.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "ff-indexer.labels" -}}
helm.sh/chart: {{ include "ff-indexer.chart" . }}
{{ include "ff-indexer.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "ff-indexer.selectorLabels" -}}
app.kubernetes.io/name: {{ include "ff-indexer.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "ff-indexer.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "ff-indexer.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
PostgreSQL hostname
*/}}
{{- define "ff-indexer.postgresql.host" -}}
{{- printf "%s-postgresql" (include "ff-indexer.fullname" .) }}
{{- end }}

{{/*
Image name helper
Usage: {{ include "ff-indexer.image" (dict "tag" .Values.api.image.tag "root" .) }}
*/}}
{{- define "ff-indexer.image" -}}
{{- $registry := .root.Values.global.imageRegistry -}}
{{- $repository := .root.Values.image.repository -}}
{{- $tag := .tag | default .root.Values.image.tag | default .root.Chart.AppVersion -}}
{{- if $registry }}
{{- printf "%s/%s:%s" $registry $repository $tag }}
{{- else }}
{{- printf "%s:%s" $repository $tag }}
{{- end }}
{{- end }}

{{/*
Common environment variables for all services
*/}}
{{- define "ff-indexer.commonEnv" -}}
- name: FF_INDEXER_DATABASE_HOST
  value: {{ include "ff-indexer.postgresql.host" . }}
- name: FF_INDEXER_DATABASE_PORT
  value: "5432"
- name: FF_INDEXER_DATABASE_USER
  value: {{ .Values.postgresql.auth.username }}
- name: FF_INDEXER_DATABASE_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ include "ff-indexer.fullname" . }}-secrets
      key: postgresql-password
- name: FF_INDEXER_DATABASE_DBNAME
  value: {{ .Values.postgresql.auth.database }}
- name: FF_INDEXER_DATABASE_SSLMODE
  value: "disable"
{{- end }}

{{/*
Temporal environment variables
*/}}
{{- define "ff-indexer.temporalEnv" -}}
- name: FF_INDEXER_TEMPORAL_HOST_PORT
  value: {{ .Values.externalTemporal.hostPort }}
- name: FF_INDEXER_TEMPORAL_NAMESPACE
  value: {{ .Values.externalTemporal.namespace }}
{{- end }}

{{/*
NATS environment variables
*/}}
{{- define "ff-indexer.natsEnv" -}}
- name: FF_INDEXER_NATS_URL
  value: {{ .Values.externalNats.url }}
{{- end }}

