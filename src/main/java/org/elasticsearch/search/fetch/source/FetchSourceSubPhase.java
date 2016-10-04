/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.fetch.source;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableList;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.search.internal.InternalSearchHitField;
import org.elasticsearch.search.SearchHitField;
import com.google.common.collect.ImmutableMap;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static com.google.common.collect.Maps.newHashMapWithExpectedSize;

/**
 */
public class FetchSourceSubPhase implements FetchSubPhase {

    @Inject
    public FetchSourceSubPhase() {

    }

    @Override
    public Map<String, ? extends SearchParseElement> parseElements() {
        ImmutableMap.Builder<String, SearchParseElement> parseElements = ImmutableMap.builder();
        parseElements.put("_source", new FetchSourceParseElement());
        return parseElements.build();
    }

    @Override
    public boolean hitsExecutionNeeded(SearchContext context) {
        return false;
    }

    @Override
    public void hitsExecute(SearchContext context, InternalSearchHit[] hits) throws ElasticsearchException {
    }

    @Override
    public boolean hitExecutionNeeded(SearchContext context) {
        return context.sourceRequested();
    }

    private void stealSourceAsFields(Map<String, Object> sourceAsMap, final Map<String, SearchHitField> fields, final Set<String> included) {
        if (sourceAsMap == null || fields == null || sourceAsMap.isEmpty() || included == null || included.isEmpty()) {
            return;
        }
        stealSourceAsFields(null, sourceAsMap, fields, included);
    }
    
    private static String appendToPath(final String path, final String subPath) {
        if (subPath == null) {
            return path;
        }
        if (path == null || path.isEmpty()) {
            return subPath;
        }
        return path + "." + subPath;
    }

    private static boolean isIncludedPath(final String path, final Set<String> included) {
        if (path != null && included != null) {
            if (included.contains(path)) return true;
            for (final String pattern : included) {
                if (Regex.simpleMatch(pattern, path)) return true;
            }
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    private static void stealSourceAsFields(final String path, final Object curObj, final Map<String, SearchHitField> fields, final Set<String> included) {
        if (isIncludedPath(path, included)) {
            // Should we always overwrite fields here?
            if (curObj instanceof List) {
                fields.put(path, new InternalSearchHitField(path, (List<Object>) curObj));
            } else {
                fields.put(path, new InternalSearchHitField(path, ImmutableList.of(curObj)));
            }
        }
        if (curObj instanceof Map<?,?>) {
            for (final Entry<String, Object> entry : ((Map<String, Object>)curObj).entrySet()) {
                stealSourceAsFields(appendToPath(path, entry.getKey()), entry.getValue(), fields, included);
            }
        }
    }
    
    @Override
    public void hitExecute(SearchContext context, HitContext hitContext) throws ElasticsearchException {
        FetchSourceContext fetchSourceContext = context.fetchSourceContext();
        assert fetchSourceContext.fetchSource();
        if (fetchSourceContext.includes().length == 0 && fetchSourceContext.excludes().length == 0) {
            hitContext.hit().sourceRef(context.lookup().source().internalSourceRef());
            return;
        }
        SourceLookup source = context.lookup().source();
        Object value = source.filter(fetchSourceContext.includes(), fetchSourceContext.excludes());
        
        if (fetchSourceContext.transformSource() || fetchSourceContext.includes().length > 0 || fetchSourceContext.excludes().length > 0) {
            // This is to maintain compatibility with 0.90
            // Copy transformed source fields as fields and flatten the results...
            if (value instanceof Map<?,?>) {
                @SuppressWarnings("unchecked")
                final Map<String, Object> sourceAsMap = (Map<String, Object>) value;
                if (!sourceAsMap.isEmpty()) {
                    final Map<String, SearchHitField> fields = newHashMapWithExpectedSize(2);
                    final Map<String, SearchHitField> sourceFields = hitContext.hit().fieldsOrNull();
                    if (sourceFields != null) {
                        fields.putAll(sourceFields);
                    }
                    final ImmutableSet<String> includedSet = ImmutableSet.<String>builder().add(fetchSourceContext.includes()).build();
                    stealSourceAsFields(sourceAsMap, fields, includedSet);
                    hitContext.hit().fields(fields);
                }
            }
        }
        XContentBuilder builder = null;
        try {
            final int initialCapacity = Math.min(1024, source.internalSourceRef().length());
            BytesStreamOutput streamOutput = new BytesStreamOutput(initialCapacity);
            builder = new XContentBuilder(context.lookup().source().sourceContentType().xContent(), streamOutput);
            builder.value(value);
            hitContext.hit().sourceRef(builder.bytes());
        } catch (IOException e) {
            throw new ElasticsearchException("Error filtering source", e);
        } finally {
            if (builder != null) {
                builder.close();
            }
        }
    }
}
