package com.mware.ge.query.builder;

import com.mware.ge.Authorizations;
import com.mware.ge.Element;
import com.mware.ge.GeObject;
import com.mware.ge.util.StreamUtils;

public class AuthorizationQueryBuilder extends GeQueryBuilder {
    private final String[] authorizations;

    protected AuthorizationQueryBuilder(String... authorizations) {
        this.authorizations = authorizations;
    }

    public String[] getAuthorizations() {
        return authorizations;
    }

    @Override
    public boolean matches(GeObject geObject, Authorizations queryAuthorizations) {
        for (String authorization : this.authorizations) {
            if (geObject instanceof Element) {
                Element element = (Element) geObject;

                if (element.getVisibility().hasAuthorization(authorization)) {
                    return true;
                }

                boolean hiddenVisibilityMatches = StreamUtils.stream(element.getHiddenVisibilities())
                        .anyMatch(visibility -> visibility.hasAuthorization(authorization));
                if (hiddenVisibilityMatches) {
                    return true;
                }
            }

            boolean propertyMatches = StreamUtils.stream(geObject.getProperties())
                    .anyMatch(property -> {
                        if (property.getVisibility().hasAuthorization(authorization)) {
                            return true;
                        }
                        return StreamUtils.stream(property.getHiddenVisibilities())
                                .anyMatch(visibility -> visibility.hasAuthorization(authorization));
                    });
            if (propertyMatches) {
                return true;
            }
        }
        return false;
    }

    @Override
    public GeQueryBuilder clone() {
        return new AuthorizationQueryBuilder(authorizations);
    }
}
