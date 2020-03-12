{{/* vim: set filetype=mustache: */}}
{{/*
Expand the name of the chart.
*/}}
{{- define "dask-gateway.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "dask-gateway.fullname" -}}
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

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "dask-gateway.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Common labels
*/}}
{{- define "dask-gateway.labels" -}}
app.kubernetes.io/name: {{ include "dask-gateway.name" . }}
helm.sh/chart: {{ include "dask-gateway.chart" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
gateway.dask.org/instance: {{ include "dask-gateway.fullname" . }}
{{- end -}}

{{/*
Match labels
*/}}
{{- define "dask-gateway.matchLabels" -}}
app.kubernetes.io/name: {{ include "dask-gateway.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
API Server name
*/}}
{{- define "dask-gateway.apiName" -}}
{{ include "dask-gateway.fullname" . | printf "api-%s" | trunc 63 | trimSuffix "-" }}
{{- end -}}

{{/*
Traefik name
*/}}
{{- define "dask-gateway.traefikName" -}}
{{ include "dask-gateway.fullname" . | printf "traefik-%s" | trunc 63 | trimSuffix "-" }}
{{- end -}}

{{/*
Controller name
*/}}
{{- define "dask-gateway.controllerName" -}}
{{ include "dask-gateway.fullname" . | printf "controller-%s" | trunc 63 | trimSuffix "-" }}
{{- end -}}
