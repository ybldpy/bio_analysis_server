package com.xjtlu.bio.response;

import java.util.List;

public record ReferenceLibraryPage(long total, int start, int pageSize, List<ReferenceLibraryItem> items) {
}
