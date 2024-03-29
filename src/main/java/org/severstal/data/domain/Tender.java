package org.severstal.data.domain;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Tender {
    private final String link;
    private final String domain;

    public Tender(String link, String domain) {
        this.link = link;
        this.domain = domain;
    }
}
